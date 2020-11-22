# bsync

Sync bucket contents to a local directory, and watch remote changes to keep
local state up to date. Currently only supports Google Cloud Storage.

Some motivating use cases are:
- As an init container to copy bucket contents for a web server.
- As a sidecar container to continuously watch for bucket changes, and update
  local assets for a web server.

## Examples

For example, here we do a oneshot sync between the source bucket and some local
directory with
```
bsync gs://BUCKET_NAME LOCAL_PATH
```

If you want to use service account credentials, then use the env var
`GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json`. If not specified
your application default credentials will be used, if they can be found.

You can also watch for change event notifications for your bucket. To do this,
you must first do the following manual steps:
1. Create a notification for your bucket `gsutil notification create -t
   TOPIC_NAME -f json gs://BUCKET_NAME` [1].
2. Create a subscription for this topic: `gcloud pubsub subscriptions create
   SUBSCRIPTION_ID --topic=TOPIC_ID` [2].

Then, you can use the `--watch` flag like
```
bsync gs://BUCKET_NAME LOCAL_PATH --watch --subscription SUBSCRIPTION_ID
```

Which will first run the oneshot sync, and then run forever watching events and
updating the local path with both new files, changes to existing files, and
removed files.

## References:
- [1] https://cloud.google.com/storage/docs/reporting-changes
- [2] https://cloud.google.com/pubsub/docs/admin#manage_subs
