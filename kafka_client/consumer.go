package main
// 普通

import (
  "github.com/Shopify/sarama"
  "log"
  "os"
)

func main() {
  consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
  if err != nil {
      panic(err)
  }

  defer func() {
      if err := consumer.Close(); err != nil {
          log.Fatalln(err)
      }
  }()

  partitionConsumer, err := consumer.ConsumePartition("myTopic", 0, sarama.OffsetNewest)
  if err != nil {
      panic(err)
  }

  defer func() {
      if err := partitionConsumer.Close(); err != nil {
          log.Fatalln(err)
      }
  }()

  // Trap SIGINT to trigger a shutdown.
  signals := make(chan os.Signal, 1)
  //signal.Notify(signals, os.Interrupt)

  consumed := 0
  ConsumerLoop:
  for {
      select {
      case msg := <-partitionConsumer.Messages():
          log.Printf("Consumed message offset:%d value==>%s\n", msg.Offset, msg.Value)
          consumed++
      case <-signals:
          break ConsumerLoop
      }
  }

  log.Printf("Consumed: %d\n", consumed)
}
