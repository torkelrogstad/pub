package main

import (
	"bytes"
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
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/pool"
	"google.golang.org/api/iterator"
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

	exists, err := t.Exists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("topic not found: %s", t.String())
	}

	msgs, err := getData(strData)
	if err != nil {
		return err
	}

	if len(msgs) == 0 {
		return errors.New("no data to publish")
	}

	if len(msgs) > 1 {
		log.Printf("publishing %d messages", len(msgs))
	}

	if duplicates := lo.FindDuplicatesBy(msgs, func(msg []byte) string {
		return string(msg)
	}); len(duplicates) > 0 {
		return fmt.Errorf("duplicates found: %v", duplicates)
	}

	start := time.Now()
	p := pool.New().WithContext(ctx)

	var bytes int
	for idx, msg := range msgs {
		bytes += len(msg)
		idx := idx
		msg := msg

		p.Go(func(ctx context.Context) error {
			msg := &pubsub.Message{
				Data: msg,
			}
			id, err := t.Publish(ctx, msg).Get(ctx)
			if err != nil {
				return fmt.Errorf("publish msg %d: %w", idx, err)
			}
			log.Printf("published msg %d: %s", idx, id)
			return nil
		})
	}

	if err := p.Wait(); err != nil {
		return err
	}

	fmt.Printf(
		"published %d bytes across %d message(s) to %s in %s\n",
		bytes, len(msgs), t.String(), time.Since(start),
	)
	return nil
}

func getData(strData string) ([][]byte, error) {
	if strData == "-" {
		log.Printf("reading data from stdin")

		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, err
		}

		lines := bytes.Split(data, []byte("\n"))

		var out [][]byte
		for idx, line := range lines {
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) == 0 {
				continue
			}

			data, err := base64.StdEncoding.DecodeString(string(trimmed))
			if err != nil {
				return nil, fmt.Errorf("decode line %d: %w", idx, err)
			}
			out = append(out, data)
		}
		return out, nil
	}

	// First try to decode the given data. If it's valid base64, we assume
	// that we should send the decoded content of that string, not the string
	// itself.
	data, err := base64.StdEncoding.DecodeString(strData)
	if err != nil {
		// Fall back to the string
		data = []byte(strData)
	}

	return [][]byte{data}, nil
}
