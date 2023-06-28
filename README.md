# pub

CLI for publishing a message to a Pub/Sub topic

## Install

```bash
$ go install github.com/torkelrogstad/pub@latest
```

## Usage

```bash
$ pub topic data
```

The Google Cloud project to operate in is determined either through the
`-project` flag, or through inspecting the output of
`gcloud config list --format 'value(core.project)'`.

If the data argument is a valid base64 byte slice, we decode it and send that
data. Otherwise, the string itself is sent.
