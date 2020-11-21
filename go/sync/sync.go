// Package sync handles reading bucket objects, and event notifications.
package sync

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	api "google.golang.org/api/storage/v1"
)

var crc32Table *crc32.Table

func init() {
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
}

type Synchronizer struct {
	log          *logrus.Logger
	cred         *google.Credentials
	projectID    string
	pubsub       *pubsub.Client
	storage      *storage.Client
	bucketName   string
	targetDir    string
	subscription string
	gens         map[string]int64
	debounce     time.Duration
}

// NewSynchronizer creates a new Synchronizer with the given src and dst.
// Credentials are automatically discovered. Credentials can also be specified
// env var GOOGLE_APPLICATION_CREDENTIALS which points to path of credentials
// JSON.
func NewSynchronizer(
	ctx context.Context,
	log *logrus.Logger,
	bucketName string,
	targetDir string,
	subscription string,
) (*Synchronizer, error) {
	cred, err := google.FindDefaultCredentials(
		ctx,
		// The best practice is to set the full cloud-platform access
		// scope on the instance, then securely limit the service
		// account's access using IAM roles.
		//
		// https://cloud.google.com/compute/docs/access/service-accounts#service_account_permissions
		"https://www.googleapis.com/auth/cloud-platform",
	)
	if err != nil {
		return nil, errors.Errorf("failed to find default credentials: %w", err)
	}
	projectID, err := getProjectID(cred)
	if err != nil {
		return nil, fmt.Errorf("failed to find project id from credentials: %w", err)
	}
	clientPubSub, err := pubsub.NewClient(
		ctx,
		projectID,
		option.WithCredentials(cred),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create new pubsub client: %w", err)
	}
	clientStorage, err := storage.NewClient(
		ctx,
		option.WithCredentials(cred),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create new storage client: %w", err)
	}
	return &Synchronizer{
		log:          log,
		cred:         cred,
		projectID:    projectID,
		pubsub:       clientPubSub,
		storage:      clientStorage,
		bucketName:   bucketName,
		targetDir:    targetDir,
		subscription: subscription,
		debounce:     1 * time.Second,
		gens:         make(map[string]int64),
	}, nil
}

func (s *Synchronizer) Sync(ctx context.Context) error {
	bucket := s.storage.Bucket(s.bucketName)
	it := bucket.Objects(ctx, nil)
	for {
		attr, err := it.Next()
		if err != nil {
			if errors.Cause(err) == iterator.Done {
				return nil
			}
			return err
		}
		dir := filepath.Join(s.targetDir, filepath.Dir(attr.Name))
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		path := filepath.Join(s.targetDir, attr.Name)

		hash, ok := crc32cFromFile(path)
		changed := hash != attr.CRC32C
		if ok && !changed {
			s.log.Infof("skipping unchanged file %s", path)
			continue
		} else if !ok {
			s.log.Infof("writing new file %s", path)
		} else if changed {
			s.log.Infof("writing changed file %s", path)
		}

		rdr, err := bucket.Object(attr.Name).NewReader(ctx)
		if err != nil {
			return err
		}
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("failed to create %s: %w", path, err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				s.log.WithError(err).Errorf("failed to close %s", path)
			}
		}()
		if _, err := io.CopyBuffer(f, rdr, nil); err != nil {
			return err
		}
	}
}

func crc32cFromFile(path string) (uint32, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return 0, false
	}
	hash := crc32.Checksum(b, crc32Table)
	return hash, true
}

func (s *Synchronizer) Watch(ctx context.Context) error {
	ch := make(chan *pubsub.Message)
	go s.applyChanges(ctx, ch)
	return s.relayMessages(ctx, ch)
}

func (s *Synchronizer) localWrite(
	ctx context.Context,
	name string,
	path string,
) error {
	bucket := s.storage.Bucket(s.bucketName)
	rdr, err := bucket.Object(name).NewReader(ctx)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", path, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			s.log.WithError(err).Errorf("failed to close %s", path)
		}
	}()
	if _, err := io.CopyBuffer(f, rdr, nil); err != nil {
		return err
	}
	return nil
}

func (s *Synchronizer) localRemove(
	name string,
) error {
	if err := os.Remove(name); err != nil {
		return err
	}
	return nil
}

func (s *Synchronizer) applyChanges(
	ctx context.Context,
	ch chan *pubsub.Message,
) {
	ticker := time.NewTicker(s.debounce)
	changed := make(map[string]objectChange)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if len(changed) > 0 {
				s.log.Debugf("flushing %d changes", len(changed))
			}
			for _, change := range changed {
				if err := s.apply(ctx, change); err != nil {
					s.log.WithError(err).Errorf(
						"failed to apply %v: %s",
						change.event,
						change.object.Name,
					)
				}
			}
			changed = make(map[string]objectChange)
		case msg, ok := <-ch:
			if !ok {
				return
			}
			s.updateChanged(msg, changed)
		}
	}
}

type objectChange struct {
	object *api.Object
	event  localEventType
}

func (s *Synchronizer) updateChanged(
	msg *pubsub.Message,
	changed map[string]objectChange,
) error {
	ty := msg.Attributes["eventType"]
	object := new(api.Object)
	if err := json.Unmarshal(msg.Data, object); err != nil {
		return err
	}
	path := filepath.Join(s.targetDir, object.Name)
	// Keep only the most up to date. Generation names are monotonically
	// increasing.
	//
	// https://cloud.google.com/storage/docs/gsutil/addlhelp/ObjectVersioningandConcurrencyControl#object-versioning
	if gen, ok := s.gens[path]; ok && gen > object.Generation {
		s.log.Debug("discarding old message object")
		return nil
	}
	s.gens[path] = object.Generation
	switch ty {
	// Finalize: Event that occurs when an object is successfully created.
	//
	// Update: Event that occurs when the metadata of an existing object
	// changes.
	case storage.ObjectFinalizeEvent, storage.ObjectMetadataUpdateEvent:
		changed[path] = objectChange{
			object: object,
			event:  localWrite,
		}
	// Delete: Event that occurs when an object is permanently deleted.
	//
	// Archive: Event that occurs when the live version of an object
	// becomes an archived version.
	//
	case storage.ObjectDeleteEvent, storage.ObjectArchiveEvent:
		// We also need to be careful of non-ordered subscriptions
		// that receive a delete after the finalize it's being overwritten by.
		//
		// https://cloud.google.com/storage/docs/pubsub-notifications#replacing_objects
		if _, ok := msg.Attributes["overwrittenByGeneration"]; ok {
			s.log.Debugf("skipping overwritten %s", ty)
			break
		}
		changed[path] = objectChange{
			object: object,
			event:  localRemove,
		}
	default:
		return errors.Errorf("unrecognized event type: %s", ty)
	}
	return nil
}

func (s *Synchronizer) relayMessages(
	ctx context.Context,
	ch chan<- *pubsub.Message,
) error {
	sub := s.pubsub.Subscription(s.subscription)
	err := sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		s.log.Debugf("received event %s", msg.Attributes["eventType"])
		ch <- msg
		msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("failed to receive from sub: %w", err)
	}
	return nil
}

func (s *Synchronizer) apply(
	ctx context.Context,
	change objectChange,
) error {
	crc32c, err := crc32cFromMessageData(change.object)
	if err != nil {
		return err
	}
	path := filepath.Join(s.targetDir, change.object.Name)
	hash, ok := crc32cFromFile(path)
	isChanged := hash != crc32c

	switch change.event {
	case localWrite:
		if ok && !isChanged {
			s.log.Infof("skipping unchanged file %s", path)
			break
		} else if !ok {
			s.log.Infof("writing new file %s", path)
		} else if isChanged {
			s.log.Infof("writing changed file %s", path)
		}
		if err := s.localWrite(ctx, change.object.Name, path); err != nil {
			return err
		}
	case localRemove:
		s.log.Infof("removing file %s", path)
		if err := s.localRemove(path); err != nil {
			return err
		}
	}

	return nil
}

type localEventType string

const (
	localWrite  localEventType = "write"
	localRemove localEventType = "remove"
)

func crc32cFromMessageData(msg *api.Object) (uint32, error) {
	b, err := base64.URLEncoding.DecodeString(msg.Crc32c)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

// TODO: Figure out a more robust way to determine the project ID from the
// credentials.
func getProjectID(cred *google.Credentials) (string, error) {
	if cred.ProjectID != "" {
		return cred.ProjectID, nil
	}
	var credQuota credentialsQuota
	if err := json.Unmarshal(cred.JSON, &credQuota); err != nil {
		return "", err
	}
	if credQuota.QuotaProjectID != "" {
		return credQuota.QuotaProjectID, nil
	}
	return "", errors.Errorf("failed to find project id")
}

type credentialsQuota struct {
	QuotaProjectID string `json:"quota_project_id"`
}
