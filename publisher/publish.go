package main

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
	// "errors"
)

const (
	QUEUE_NAME  = "que"
	EXCHANGE    = "nex_ex"
	ROUTING_KEY = "rout"
)

type Publisher struct {
	url                    string
	conn                   *amqp.Connection
	channels               map[string]*amqp.Channel
	mutex                  *sync.Mutex
	ConnectionCloseTimeout int
	MaxRecoveryAttempts    int
}

func NewPublisher(url string) (Publisher, error) {
	if url == "" {
		return Publisher{}, errors.New("url is not set")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return Publisher{}, err
	}
	log.Println("Publisher connected")
	return Publisher{
		url:                    url,
		conn:                   conn,
		channels:               make(map[string]*amqp.Channel),
		mutex:                  &sync.Mutex{},
		ConnectionCloseTimeout: 5,
		MaxRecoveryAttempts:    3,
	}, nil
}

func (pb *Publisher) Publish(body []byte, routingKey string, exchange string, th2Pin string, th2Type string) error {
	ch, err := pb.getChannel(routingKey)
	if err != nil {
		log.Printf("Failed toget channel:", err)
		return err
	}

	connErr := make(chan *amqp.Error, 1)
	chanErr := make(chan *amqp.Error, 1)

	go func(cn chan *amqp.Error) {
		log.Println("in cON")

		//cn <- <-pb.conn.NotifyClose(make(chan *amqp.Error, 1)) //Listen to NotifyClose
		cn <- &amqp.Error{Code: amqp.AccessRefused} //for test

	}(connErr)

	go func(chn chan *amqp.Error) {
		//log.Println("in chaN")
		//chn <- <-pb.channels[routingKey].NotifyClose(make(chan *amqp.Error, 1))
		chn <- &amqp.Error{Code: amqp.AccessRefused} //for testing

	}(chanErr)
	pb.conn.Close()
	for {
		publError := ch.Publish(exchange, routingKey, false, false, amqp.Publishing{Body: body})
		if publError != nil {
			log.Println(publError)
			//return nil
		}
		errH := pb.connErrorHandling(connErr)
		if errH != nil {
			log.Println("Failed to publish message to RabbitMQ:")
		}
		errch := pb.chanErrorHandling(chanErr, routingKey)
		if errch != nil {
			log.Println("Failed to recover channel")

		}

	}
	//bodySize := len(body)
	//loger.Trace().Int("size", bodySize).Msg("data published")
}

func (pb *Publisher) Close() error {
	return pb.conn.Close()
}

func (pb *Publisher) getChannel(routingKey string) (*amqp.Channel, error) {
	var ch *amqp.Channel
	var err error
	var exists bool
	ch, exists = pb.channels[routingKey]
	if !exists {
		ch, err = pb.getOrCreateChannel(routingKey)
	}

	return ch, err
}

func (pb *Publisher) getOrCreateChannel(routingKey string) (*amqp.Channel, error) {
	pb.mutex.Lock()
	defer pb.mutex.Unlock()
	var ch *amqp.Channel
	var err error
	var exists bool
	ch, exists = pb.channels[routingKey]
	if !exists {
		ch, err = pb.conn.Channel()
		if err != nil {
			return nil, err
		}
		pb.channels[routingKey] = ch
	}
	return ch, nil
}
func (pb *Publisher) connErrorHandling(ch chan *amqp.Error) error {
	res := make(chan error, 1)
	go func() {
		res <- pb.handleConnectionErr(ch)
	}()
	select {
	case <-time.After(time.Duration(pb.ConnectionCloseTimeout) * time.Second):
		log.Panicln("timed out")
	case result := <-res:
		return result
	}
	return nil
}

func (pb *Publisher) handleConnectionErr(connErr chan *amqp.Error) error {
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
		errR := pb.reconnect()
		if errR != nil {
			return errR
		}

	default:
		return nil
	}
	return nil
}
func (pb *Publisher) reconnect() error {
	log.Println("reconnecting..")
	//time.Sleep(time.Duration(cn.ConnectionCloseTimeout) * time.Second) //for testing
	var err error
	for i := 0; i < pb.MaxRecoveryAttempts; i++ {
		time.Sleep(time.Duration(i*2) * time.Second)
		var con *amqp.Connection
		con, err := amqp.Dial(pb.url)
		if err == nil {
			pb.conn = con
			log.Println("reconnected")
			return nil
		}
	}
	return err
}
func (pb *Publisher) handleChannelErrors(chn chan *amqp.Error, name string) error {

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
			for i := 0; i < pb.MaxRecoveryAttempts; i++ {

				// Attempt to re-establish the channel
				//time.Sleep(time.Duration(cn.ConnectionCloseTimeout) * time.Second)//test
				var chann *amqp.Channel
				chann, err = pb.conn.Channel()
				log.Printf("staret recreating %v", err)

				if err == nil {
					pb.channels[name] = chann
					log.Printf("Channel recovered after error: %v", err)
					return nil
				}
			}
			return err
		}
	}

}
func (pb *Publisher) chanErrorHandling(ch chan *amqp.Error, name string) error {

	res := make(chan error, 1)
	go func() {
		res <- pb.handleChannelErrors(ch, name)
	}()
	select {
	case <-time.After(time.Duration(pb.ConnectionCloseTimeout) * time.Second):
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
	connector, _ := NewPublisher("amqp://guest:guest@localhost:5672/")
	body := []byte("Hello, world!")

	err := connector.Publish(body, ROUTING_KEY, EXCHANGE, "pin2", "smth")
	if err != nil {
		log.Println(err)
	}

}
