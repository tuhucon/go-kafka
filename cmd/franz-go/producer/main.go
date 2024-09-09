package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/twmb/franz-go/pkg/kgo"
	"kafka/internal"
	"log/slog"
	"os"
	"strconv"
)

func main() {
	numMsgs := flag.Int("count", 1, "Number of messages to send")
	flag.Parse()

	if *numMsgs < 1 {
		slog.Error("Number of messages must be greater than zero")
		os.Exit(1)
	}

	seeds := []string{"localhost:9092"}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	slog.Info("public messages to kafka", "count", *numMsgs)
	for i := 1; i <= *numMsgs; i++ {
		person := internal.Person{
			Id:      i,
			Name:    "Tu hu con",
			Age:     i,
			Address: "Ha Noi",
		}
		value, err := json.Marshal(person)
		if err != nil {
			slog.Error("cannot json marshal person", "person", person, "err", err)
			continue
		}
		record := &kgo.Record{
			Key:   []byte(strconv.Itoa(person.Id)),
			Value: value,
			Topic: "test",
		}
		if client.ProduceSync(context.Background(), record).FirstErr() != nil {
			slog.Error("cannot produce record", "person", person, "err", err)
			continue
		}
		slog.Info("produced record", "person", person)
	}
}
