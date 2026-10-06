package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionURl := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(connectionURl)
	if err != nil {
		log.Fatalf("flop connecting to message broker: %s", err.Error())
	}
	defer connection.Close()

	fmt.Println("Connection successful")

	channel, err := connection.Channel()
	if err != nil {
		return
	}

	err = pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.Durable, handlerLog())
	if err != nil {
		fmt.Printf("Flop subscribingGob: %s\n", err.Error())
		return
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		userCommand := input[0]

		switch userCommand {
		case "pause":
			fmt.Println("Pausing...")
			err := pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				fmt.Printf("Flop pausing: %s", err.Error())
			}
			break

		case "resume":
			fmt.Println("Resuming...")
			err := pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				fmt.Printf("Flop pausing: %s", err.Error())
			}
			break

		case "quit":
			gamelogic.PrintQuit()
			break

		default:
			fmt.Println("flop command, mysterious in this land.")
			break
		}
	}

	osSignalsChan := make(chan os.Signal, 1)
	signal.Notify(osSignalsChan, os.Interrupt)
	<-osSignalsChan
}
