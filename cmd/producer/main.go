package main

import (
	"encoding/json"
	"flag"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafka/internal"
	"log/slog"
	"os"
	"strconv"
	"time"
)

func main() {
	jsonLog := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(jsonLog)

	numMsgsFlag := flag.Int("n", 1, "Number of messages to send")
	flag.Parse()

	if *numMsgsFlag <= 0 {
		panic("number of messages must be greater than zero")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// Listen to events got after sending messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					slog.Error("Delivery failed", "topic partition", ev.TopicPartition)
				} else {
					slog.Info("Delivered successfully", "topic partition", ev.TopicPartition)
				}
			case kafka.Error:
				slog.Warn("Error", "error", ev)
			default:
				slog.Info("ignored event", "event", ev)
			}
		}
	}()

	// Publish message
	topic := "test"
	for i := 1; i <= *numMsgsFlag; i++ {
		person := internal.Person{Id: i, Name: "tu hu con", Age: 12, Address: "ha noi"}
		eventChan := make(chan kafka.Event)
		value, err := json.Marshal(person)
		if err != nil {
			slog.Error("Error marshalling person", "error", err)
			continue
		}
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(strconv.Itoa(person.Id)),
			Value:          value,
		}, eventChan)
		if err != nil {
			slog.Error("Error producing message", "error", err)
			continue
		}
		// Publish message in sync
		p.Events() <- <-eventChan
		close(eventChan)
	}
	// Flush publisher and wait
	p.Flush(10_000)
	time.Sleep(1 * time.Second)
}
