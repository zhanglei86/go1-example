package main
// group方式

import (
  "github.com/Shopify/sarama"
  "context"
  "fmt"
)

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        fmt.Printf("Message topic:%q partition:%d offset:%d value==>%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
        sess.MarkMessage(msg, "")
    }
    return nil
}

func main() {
    // Init config, specify appropriate version
    config := sarama.NewConfig()
    config.Version = sarama.V1_0_0_0
    config.Consumer.Return.Errors = true

    // Start with a client
    client, err := sarama.NewClient([]string{"localhost:9092"}, config)
    if err != nil {
        panic(err)
    }
    defer func() { _ = client.Close() }()

    // Start a new consumer group
    group, err := sarama.NewConsumerGroupFromClient("myGroup", client)
    if err != nil {
        panic(err)
    }
    defer func() { _ = group.Close() }()

    // Track errors
    go func() {
        for err := range group.Errors() {
            fmt.Println("ERROR", err)
        }
    }()

    // Iterate over consumer sessions.
    ctx := context.Background()
    for {
        topics := []string{"myTopic"}
        handler := exampleConsumerGroupHandler{}

        err := group.Consume(ctx, topics, handler)
        if err != nil {
            panic(err)
        }
    }
}
