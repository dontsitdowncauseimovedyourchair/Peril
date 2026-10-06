package pubsub

import amqp "github.com/rabbitmq/amqp091-go"

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	chann, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	args := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}
	var queue amqp.Queue
	switch queueType {
	case Durable:
		queue, err = chann.QueueDeclare(queueName, true, false, false, false, args)
		if err != nil {
			return nil, amqp.Queue{}, err
		}
		break
	case Transient:
		queue, err = chann.QueueDeclare(queueName, false, true, true, false, args)
		if err != nil {
			return nil, amqp.Queue{}, err
		}
		break
	}

	err = chann.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return chann, queue, nil
}
