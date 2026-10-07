package main

import (
	"fmt"
	"strconv"
	"time"

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

func handleWar(gs *gamelogic.GameState, chann *amqp.Channel) func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		if outcome == gamelogic.WarOutcomeNotInvolved {
			return pubsub.NackRequeue
		} else if outcome == gamelogic.WarOutcomeNoUnits {
			return pubsub.NackDiscard
		} else if outcome == gamelogic.WarOutcomeOpponentWon {
			err := pubsub.PublishLog(chann, routing.GameLog{
				CurrentTime: time.Time{},
				Message:     fmt.Sprintf("%s won a war against %s", winner, loser),
				Username:    gs.Player.Username,
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		} else if outcome == gamelogic.WarOutcomeYouWon {
			err := pubsub.PublishLog(chann, routing.GameLog{
				CurrentTime: time.Now(),
				Message:     fmt.Sprintf("%s won a war against %s", winner, loser),
				Username:    gs.Player.Username,
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		} else if outcome == gamelogic.WarOutcomeDraw {
			err := pubsub.PublishLog(chann, routing.GameLog{
				CurrentTime: time.Now(),
				Message:     fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
				Username:    gs.Player.Username,
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}

		fmt.Printf("outcome not known: %v\n", outcome)
		return pubsub.NackDiscard
	}
}

func handleSpam(chann *amqp.Channel, username string, input []string) error {
	if len(input) != 2 {
		return fmt.Errorf("usage: spam <n>")
	}
	n, err := strconv.Atoi(input[1])
	if err != nil {
		return err
	}

	for _ = range n {
		maliciousLog := gamelogic.GetMaliciousLog()
		err := pubsub.PublishLog(chann, routing.GameLog{
			CurrentTime: time.Now(),
			Message:     maliciousLog,
			Username:    username,
		})
		if err != nil {
			fmt.Printf("Flop publishing malicious log: %s\n", err.Error())
			continue
		}
	}

	return nil
}
