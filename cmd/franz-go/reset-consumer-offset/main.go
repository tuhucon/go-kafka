package main

import (
	"context"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"log/slog"
)

func main() {
	seeds := []string{"localhost:9092"}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	admClient := kadm.NewClient(client)
	group := "test-consumer"
	topic := "test"

	offsets, err := admClient.FetchOffsets(context.Background(), group)
	if err != nil {
		panic(err)
	}

	newOffsets := make(kadm.Offsets)
	for tp, offset := range offsets.Offsets() {
		if tp == topic {
			for _, parOff := range offset {
				slog.Info("offset information", "parOff", parOff)
				newParOff := kadm.Offset{
					Topic:     parOff.Topic,
					Partition: parOff.Partition,
					At:        parOff.At - 10,
				}
				if newParOff.At < 0 {
					newParOff.At = 0
				}
				newOffsets.Add(newParOff)
			}
		}
	}

	err = admClient.CommitAllOffsets(context.Background(), group, newOffsets)
	if err != nil {
		panic(err)
	}
}
