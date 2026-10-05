package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionURl := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(connectionURl)
	if err != nil {
		log.Fatalf("flop connecting to message broker: %s", err.Error())
	}
	defer connection.Close()

	fmt.Println("Connection successful")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("flop welcoming: %s", err.Error())
	}

	userQuit := false

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.SimpleQueueType(1), handlerPause(gs))
	if err != nil {
		fmt.Printf("Flop subscribing to %s: %s", routing.ExchangePerilDirect, err.Error())
		return
	}

	for {
		if userQuit {
			break
		}

		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		userCommand := input[0]

		switch userCommand {
		case "spawn":
			err := gs.CommandSpawn(input)
			if err != nil {
				fmt.Printf("flop spawning: %s\n", err.Error())
			}
			break
		case "move":
			_, err := gs.CommandMove(input)
			if err != nil {
				fmt.Printf("flop moving: %s\n", err.Error())
			}
			break

		case "status":
			gs.CommandStatus()
			break

		case "help":
			gamelogic.PrintClientHelp()
			break

		case "spam":
			fmt.Println("Spamming not allowed... yet")
			break

		case "quit":
			gamelogic.PrintQuit()
			userQuit = true
			break

		default:
			fmt.Println("Your troops are oblivious to such command")
			continue
		}
	}
}
