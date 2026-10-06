package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
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
			ackt := handler(decoded)
			switch ackt {
			case Ack:
				delivery.Ack(false)
				if err != nil {
					fmt.Println("Flop ack!")
				} else {
					fmt.Println("acked!!")
				}
				break
			case NackRequeue:
				delivery.Nack(false, true)
				if err != nil {
					fmt.Println("Flop nackrequeuing!")
				} else {
					fmt.Println("nackrequeued!")
				}
				break
			case NackDiscard:
				err = delivery.Nack(false, false)
				if err != nil {
					fmt.Println("Flop nackdiscarding!")
				} else {
					fmt.Println("nackdiscarded!")
				}
				break
			}
			if err != nil {
				log.Fatalf("flop acknowledging delivery: %s", err.Error())
			}
		}
	}()

	return nil
}
