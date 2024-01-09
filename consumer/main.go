package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "golang",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}

	defer consumer.Close()

	err = consumer.Subscribe("helloworld", nil)

	if err != nil {
		panic(err)
	}

	for {
		message, err := consumer.ReadMessage(1 * time.Second)

		if err == nil {
			fmt.Printf("Receive key : %s\n", message.Key)
			fmt.Printf("Receive message : %s\n", message.Value)
		}
	}
}
