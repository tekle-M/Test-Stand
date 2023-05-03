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
	pb.channels[routingKey] = ch

	connErr := make(chan *amqp.Error, 1)
	go func(cn chan<- *amqp.Error) {
		cn <- <-pb.conn.NotifyClose(make(chan *amqp.Error, 1)) //Listen to NotifyClose
		//cn <- &amqp.Error{Code: amqp.AccessRefused} //for test

	}(connErr)

	//pb.conn.Close()
	for i := 0; i < pb.MaxRecoveryAttempts; i++ {
		select {
		case errRes := <-connErr:
			errH := pb.connErrorHandling(errRes, routingKey)
			if errH != nil {
				log.Println("Failed to publish message to RabbitMQ:")
				return errH
			}
		default:
		}
		publError := pb.channels[routingKey].Publish(exchange, routingKey, false, false, amqp.Publishing{Body: body})
		if publError == nil {
			log.Println("published")
			break
		}
		log.Println(publError)

	}
	//bodySize := len(body)
	//loger.Trace().Int("size", bodySize).Msg("data published")
	return nil

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
func (pb *Publisher) connErrorHandling(ch *amqp.Error, name string) error {
	res := make(chan error, 1)
	go func() {
		res <- pb.handleConnectionErr(ch, name)
	}()
	select {
	case <-time.After(time.Duration(pb.ConnectionCloseTimeout) * time.Second):
		log.Panicln("timed out")
	case result := <-res:
		return result
	}
	return nil
}

func (pb *Publisher) handleConnectionErr(connErr *amqp.Error, name string) error {
	switch connErr.Code {
	case amqp.AccessRefused:
		log.Println("Access refused error:", connErr)
	case amqp.ConnectionForced:
		log.Println("Connection forced error:", connErr)
	default:
		log.Println("Unknown error:", connErr)
	}
	errR := pb.reconnect()
	if errR != nil {
		return errR
	}
	pb.mutex.Lock()
	defer pb.mutex.Unlock()
	chann, err := pb.conn.Channel()
	if err != nil {
		return err
	}
	pb.channels[name] = chann
	log.Println("Channel recovered")
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

func main() {
	connector, _ := NewPublisher("amqp://guest:guest@localhost:5672/")
	body := []byte("Hello, world!")

	err := connector.Publish(body, ROUTING_KEY, EXCHANGE, "pin2", "smth")
	if err != nil {
		log.Println(err)
	}

}
