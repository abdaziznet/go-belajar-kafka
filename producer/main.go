package main

import (
	"fmt"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := kafka.NewProducer(config)

	if err != nil {
		panic(err)
	}
	defer producer.Close()

	topic := "helloworld"

	for i := 10; i < 20; i++ {
		fmt.Printf("send message to kafka %d \n", i)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(fmt.Sprintf("Hello %d", i)),
		}

		err = producer.Produce(msg, nil)
		if err != nil {
			panic(err)
		}
	}
}
