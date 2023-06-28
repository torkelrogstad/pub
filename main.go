package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
)

var (
	projectID  = flag.String("project", "", "Google project ID (defaults to gcloud settings)")
	listTopics = flag.Bool("list", false, "list available topics")
)

func main() {
	if err := realMain(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", os.Args[0], err)
		os.Exit(1)
	}
}

func getProject(ctx context.Context) (string, error) {
	if *projectID != "" {
		return *projectID, nil
	}

	cmd := exec.CommandContext(ctx, "gcloud", "config", "list", "--format", "value(core.project)")
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

func doListTopics(ctx context.Context, client *pubsub.Client) error {
	it := client.Topics(ctx)
	for {
		topic, err := it.Next()
		switch {
		case errors.Is(err, iterator.Done):
			return nil

		case err != nil:
			return err
		}

		_, _ = fmt.Fprintln(os.Stdout, topic.ID())
	}
}

func realMain() error {
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	project, err := getProject(ctx)
	if err != nil {
		return err
	}

	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return err
	}

	if *listTopics {
		return doListTopics(ctx, client)
	}

	topic := flag.Arg(0)
	if topic == "" {
		return errors.New("expects topic as first argument")
	}

	strData := flag.Arg(1)
	if strData == "" {
		return errors.New("expects data as second argument")
	}

	// First try to decode the given data. If it's valid base64, we assume
	// that we should send the decoded content of that string, not the string
	// itself.
	data, err := base64.StdEncoding.DecodeString(strData)
	if err != nil {
		// Fall back to the string
		data = []byte(strData)
	}

	t := client.Topic(topic)
	res := t.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	serverID, err := res.Get(ctx)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	fmt.Printf("published %d bytes to %s: server ID %s\n", len(data), t.String(), serverID)
	return nil
}
