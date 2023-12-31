package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	projectID  = flag.String("project", "", "Google project ID (defaults to gcloud settings)")
	listTopics = flag.Bool("list", false, "list available topics")
	verbose    = flag.Bool("v", false, "verbose output")
)

func main() {
	if err := realMain(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", os.Args[0], err)
		os.Exit(1)
	}
}

func getProject(ctx context.Context) (string, error) {
	log.SetFlags(log.Lmicroseconds)
	if !*verbose {
		log.SetOutput(io.Discard)
	}

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
	flag.Usage = func() {
		fmt.Println()
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s: <topic> <base64_data>\n", os.Args[0])

	}
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	project, err := getProject(ctx)
	if err != nil {
		return err
	}
	log.Printf("project: %s", project)

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
	if strings.Contains(topic, "/") {
		log.Printf("assuming topic is full path: %s", topic)
		parts := strings.Split(topic, "/")
		if len(parts) != 4 {
			return fmt.Errorf("invalid format: expects projects/PROJECT_ID/topics/NAME: %w", err)
		}

		project := parts[1]
		topic := parts[3]
		t = client.TopicInProject(topic, project)
	}

	res := t.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	serverID, err := res.Get(ctx)
	switch {
	case status.Code(err) == codes.NotFound:
		return fmt.Errorf("publish: topic not found: %s", t.ID())

	case err != nil:
		return fmt.Errorf("publish: %w", err)
	}

	fmt.Printf("published %d bytes to %s: server ID %s\n", len(data), t.String(), serverID)
	return nil
}
