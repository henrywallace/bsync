package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/henrywallace/bsync/go/sync"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var root *cobra.Command

func init() {
	root = cmdRoot()
}

func cmdRoot() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "bsync BUCKET_NAME LOCAL_PATH",
		RunE: runRoot,
		Args: cobra.ExactArgs(2),
	}
	flags := cmd.Flags()
	flags.BoolP(
		"watch",
		"w",
		false,
		"whether to watch for incremental updates",
	)
	flags.String(
		"log-level",
		"info",
		"which level to log at",
	)
	flags.String(
		"subscription",
		"",
		"id of subscription to watch for events",
	)
	return cmd
}

type rootConfig struct {
	log          *logrus.Logger
	bucketName   string
	targetDir    string
	subscription string
	watch        bool
}

func newRootConfig(cmd *cobra.Command, args []string) (*rootConfig, error) {
	flags := cmd.Flags()

	lvlStr, err := flags.GetString("log-level")
	if err != nil {
		panic(err)
	}
	lvl, err := logrus.ParseLevel(lvlStr)
	if err != nil {
		return nil, err
	}
	log := logrus.New()
	log.SetLevel(lvl)

	bucketName := args[0]
	bucketName = strings.TrimPrefix(bucketName, "gs://")

	targetDir := args[1]

	watch, err := flags.GetBool("watch")
	if err != nil {
		panic(err)
	}

	subscription, err := flags.GetString("subscription")
	if err != nil {
		panic(err)
	}
	if watch && subscription == "" {
		return nil, fmt.Errorf("must provide --subscription with --watch")
	}

	return &rootConfig{
		log:          log,
		bucketName:   bucketName,
		subscription: subscription,
		targetDir:    targetDir,
		watch:        watch,
	}, nil
}

func runRoot(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	conf, err := newRootConfig(cmd, args)
	if err != nil {
		return err
	}

	s, err := sync.NewSynchronizer(
		ctx,
		conf.log,
		conf.bucketName,
		conf.targetDir,
		conf.subscription,
	)
	if err != nil {
		return err
	}
	if err := s.Sync(ctx); err != nil {
		return err
	}
	if conf.watch {
		if err := s.Watch(ctx); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
