package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
)

// Consume messages from queue
func Consume() {
	// Create a new configuration instance
	config := sarama.NewConfig()

	// Specify brokers address. 9092 is default
	brokers := []string{"localhost:9092"}

	// Create a new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	// Wait to close after everything is processed
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	// Topic to consume
	topic := "influx-topic"

	// ConsumePartition creates a PartitionConsumer on the given topic/partition with the given offset
	// A PartitionConsumer processes messages from a given topic and partition
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	// Stop process if connection is interrupted
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Signal to finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			// In case of error
			case err := <-consumer.Errors():
				fmt.Println(err)
			// Print consumer messages
			case msg := <-consumer.Messages():
				fmt.Println(string(msg.Value))
			// Service interruption
			case <-signals:
				fmt.Println("Interrupt detected")
				doneCh <- struct{}{}
			}
		}
	}()

	// If everything is done, close consumer
	<-doneCh
	fmt.Println("Consumption closed")
}

func main() {
	Consume()
}
