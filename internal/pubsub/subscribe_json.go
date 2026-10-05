package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	chann, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := chann.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			var decoded T
			err := json.Unmarshal(delivery.Body, &decoded)
			if err != nil {
				log.Printf("flop unmarshalling: %s\n", err.Error())
				return
			}
			handler(decoded)
			err = delivery.Ack(false)
			if err != nil {
				log.Fatalf("flop acknowledging delivery: %s", err.Error())
			}
		}
	}()

	return nil
}
