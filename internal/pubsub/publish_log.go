package pubsub

import (
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishLog(chann *amqp.Channel, log routing.GameLog) error {
	exchange := routing.ExchangePerilTopic
	key := routing.GameLogSlug + "." + log.Username

	err := PublishGob(chann, exchange, key, log)
	if err != nil {
		return err
	}
	return nil
}
