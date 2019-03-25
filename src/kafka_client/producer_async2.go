package main

// select方式

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
)

func main() {
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
ProducerLoop:
	for num := 0; num < 10; num++ {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "myTopic", Key: nil, Value: sarama.StringEncoder("testing123")}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
