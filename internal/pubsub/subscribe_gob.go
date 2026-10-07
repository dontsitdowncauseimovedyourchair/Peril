package pubsub

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType) error {

	return subscribe(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		var msg T
		err := dec.Decode(&msg)
		if err != nil {
			return msg, err
		}
		return msg, nil
	})
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	chann, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	err = chann.Qos(10, 0, false)
	if err != nil {
		return err
	}

	deliveryChan, err := chann.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			msg, err := unmarshaller(delivery.Body)
			if err != nil {
				fmt.Printf("Flop decoding: %s\n", err.Error())
				continue
			}
			ackt := handler(msg)
			switch ackt {
			case Ack:
				err = delivery.Ack(false)
				if err != nil {
					fmt.Println("Flop ack!")
				} else {
					fmt.Println("acked!!")
				}
				break
			case NackRequeue:
				err = delivery.Nack(false, true)
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
