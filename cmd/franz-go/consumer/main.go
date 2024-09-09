package main

import (
	"context"
	"encoding/json"
	"github.com/twmb/franz-go/pkg/kgo"
	"kafka/internal"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	seeds := []string{"localhost:9092"}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("test-consumer"),
		kgo.ConsumeTopics("test"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for {
		select {
		case sig := <-sigchan:
			slog.Warn("got signal", "signal", sig)
			os.Exit(1)
		default:
			fetches := client.PollRecords(context.Background(), 10)
			for _, fetch := range fetches {
				for _, topic := range fetch.Topics {
					for _, partition := range topic.Partitions {
						persons := make(map[int]*internal.Person, len(partition.Records))
						// Aggregate person by id for batch processing
						for _, record := range partition.Records {
							person := new(internal.Person)
							err := json.Unmarshal(record.Value, person)
							if err != nil {
								slog.Warn("failed to unmarshal record", "record", string(record.Value))
							}
							persons[person.Id] = person
						}
						for _, person := range persons {
							slog.Info("processed person", "person", person)
						}
					}
				}
			}
		}
	}
}
