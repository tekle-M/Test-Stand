package main

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"time"
	// "errors"
)

const (
	QUEUE_NAME  = "que"
	EXCHANGE    = "nex_ex"
	ROUTING_KEY = "rout"
)

type Consumer struct {
	url                    string
	conn                   *amqp.Connection
	channels               map[string]*amqp.Channel
	ConnectionCloseTimeout int
	MaxRecoveryAttempts    int
}

func NewConsumer(url string) (Consumer, error) {
	if url == "" {
		return Consumer{}, errors.New("url is empty")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return Consumer{}, err
	}
	log.Println("Consumer connected")
	return Consumer{
		url:      url,
		conn:     conn,
		channels: make(map[string]*amqp.Channel),
	}, nil
}

func (cns *Consumer) Consume(queueName string, th2Pin string, th2Type string, handler func(delivery amqp.Delivery) error) error {

	connErr := make(chan *amqp.Error, 1)
	chanErr := make(chan *amqp.Error, 1)

	go func(cn chan *amqp.Error) {
		log.Println("in cON")

		//cn <- <-pb.conn.NotifyClose(make(chan *amqp.Error, 1)) //Listen to NotifyClose
		cn <- &amqp.Error{Code: amqp.AccessRefused} //for test

	}(connErr)

	ch, err := cns.conn.Channel()
	if err != nil {
		log.Println("cannot open channel")
		return err
	}
	cns.channels[queueName] = ch
	var msgs <-chan amqp.Delivery
	for i := 0; i < cns.MaxRecoveryAttempts; i++ {
		var consErr error
		msgs, consErr = ch.Consume(
			queueName, // queue
			// TODO: we need to provide a name that will help to identify the component
			"",    // consumer
			true,  // auto-ack
			false, // exclusive
			false, // no-local
			false, // no-wait
			nil,   // args
		)
		if consErr == nil {
			log.Println("Consuming error")
			return consErr
		}
		errH := cns.connErrorHandling(connErr)
		if errH != nil {
			log.Println("Failed to publish message to RabbitMQ:")
		}
		errch := cns.chanErrorHandling(chanErr, queueName)
		if errch != nil {
			log.Println("Failed to recover channel")

		}

	}

	go func() {
		log.Println("start handling messages")
		for d := range msgs {
			log.Println("receive delivery")
			if err := handler(d); err != nil {
				log.Println("Cannot handle delivery")
			}
		}
		log.Println("stop handling messages")
	}()

	return nil
}
func (cns *Consumer) Close() error {
	return cns.conn.Close()
}

func (cns *Consumer) connErrorHandling(ch chan *amqp.Error) error {
	res := make(chan error, 1)
	go func() {
		res <- cns.handleConnectionErr(ch)
	}()
	select {
	case <-time.After(time.Duration(cns.ConnectionCloseTimeout) * time.Second):
		log.Panicln("timed out")
	case result := <-res:
		return result
	}
	return nil
}

func (cns *Consumer) handleConnectionErr(connErr chan *amqp.Error) error {
	select {
	case err := <-connErr:
		switch err.Code {
		case amqp.AccessRefused:
			log.Println("Access refused error:", err)
		case amqp.ConnectionForced:
			log.Println("Connection forced error:", err)
		default:
			log.Println("Unknown error:", err)
		}
		errR := cns.reconnect()
		if errR != nil {
			return errR
		}

	default:
		return nil
	}
	return nil
}
func (cns *Consumer) reconnect() error {
	log.Println("reconnecting..")
	//time.Sleep(time.Duration(cn.ConnectionCloseTimeout) * time.Second) //for testing
	var err error
	for i := 0; i < cns.MaxRecoveryAttempts; i++ {
		time.Sleep(time.Duration(i*2) * time.Second)
		var con *amqp.Connection
		con, err := amqp.Dial(cns.url)
		if err == nil {
			cns.conn = con
			log.Println("reconnected")
			return nil
		}
	}
	return err
}
func (cns *Consumer) handleChannelErrors(chn chan *amqp.Error, name string) error {

	for {
		errC := <-chn
		if errC != nil {
			log.Printf("Channel closed with error: %v, attempting to recover...", errC)
			switch errC.Code {
			case amqp.AccessRefused:
				log.Println("access refused error, retrying...")
				// here might go mechanism to solver this error if it is possible to do with code

			case amqp.ChannelError:
				log.Println("channel error, retrying in 5 seconds...")

			default:
				log.Printf("unknown error code: %d, retrying in 1 second...", errC.Code)
			}
			var err error
			for i := 0; i < cns.MaxRecoveryAttempts; i++ {

				// Attempt to re-establish the channel
				//time.Sleep(time.Duration(cn.ConnectionCloseTimeout) * time.Second)//test
				var chann *amqp.Channel
				chann, err = cns.conn.Channel()
				log.Printf("staret recreating %v", err)

				if err == nil {
					cns.channels[name] = chann
					log.Printf("Channel recovered after error: %v", err)
					return nil
				}
			}
			return err
		}
	}

}
func (cns *Consumer) chanErrorHandling(ch chan *amqp.Error, name string) error {

	res := make(chan error, 1)
	go func() {
		res <- cns.handleChannelErrors(ch, name)
	}()
	select {
	case <-time.After(time.Duration(cns.ConnectionCloseTimeout) * time.Second):
		//return errors.New("timed out")
		log.Panicln("timed out")
	case result := <-res:
		return result
	default:
		return nil
	}
	return nil
}
func main() {
	connector, _ := NewConsumer("amqp://guest:guest@localhost:5672/")
	err := connector.Close()
	if err != nil {
		log.Println(err)
	}
	
}
