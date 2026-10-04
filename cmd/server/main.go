package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionURl := "amqp://guest:guest@localhost:5672/"

	dial, err := amqp.Dial(connectionURl)
	if err != nil {
		log.Fatalf("flop connecting to message broker: %s", err.Error())
	}
	defer dial.Close()

	fmt.Println("Connection successful")

	osSignalsChan := make(chan os.Signal, 1)
	signal.Notify(osSignalsChan, os.Interrupt)
	<-osSignalsChan

	gamelogic.PrintQuit()
}
