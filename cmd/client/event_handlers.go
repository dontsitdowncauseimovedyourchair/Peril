package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handleMove(gs *gamelogic.GameState, chann *amqp.Channel) func(move gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		if outcome == gamelogic.MoveOutcomeSamePlayer {
			return pubsub.NackDiscard
		} else if outcome == gamelogic.MoveOutComeSafe {
			return pubsub.Ack
		} else if outcome == gamelogic.MoveOutcomeMakeWar {
			err := pubsub.PublishJSON(chann, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.GetPlayerSnap().Username, gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Println("flop publishing war")
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		} else {
			fmt.Printf("unknown outcome: %v\n", outcome)
			return pubsub.NackDiscard
		}
	}
}

func handleWar(gs *gamelogic.GameState) func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, _, _ := gs.HandleWar(rw)
		if outcome == gamelogic.WarOutcomeNotInvolved {
			return pubsub.NackRequeue
		} else if outcome == gamelogic.WarOutcomeNoUnits {
			return pubsub.NackDiscard
		} else if outcome == gamelogic.WarOutcomeOpponentWon {
			return pubsub.Ack
		} else if outcome == gamelogic.WarOutcomeYouWon {
			return pubsub.Ack
		} else if outcome == gamelogic.WarOutcomeDraw {
			return pubsub.Ack
		}

		fmt.Printf("outcome not known: %v\n", outcome)
		return pubsub.NackDiscard
	}
}
