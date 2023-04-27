package main

import (
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

type Connector struct {
	url                    string
	conn                   *amqp.Connection
	channels               map[string]*amqp.Channel
	connErr                chan *amqp.Error
	chanErr                chan *amqp.Error
	ConnectionTimeout      int
	ConnectionCloseTimeout int
	MaxRecoveryAttempts    int
}

func GetConnector() Connector {
	return Connector{url: "amqp://guest:guest@localhost:5672/",
		channels: map[string]*amqp.Channel{}, connErr: make(chan *amqp.Error, 1), chanErr: make(chan *amqp.Error, 1),
		ConnectionCloseTimeout: 7, MaxRecoveryAttempts: 3}
}

func (cn *Connector) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(cn.url)
	if err != nil {
		return nil, err
	}
	cn.conn = conn

	log.Println("connected")
	return conn, nil
}

func (cn *Connector) reconnect() error {
	log.Println("reconnecting..")
	//time.Sleep(time.Duration(cn.ConnectionCloseTimeout) * time.Second) //for testing
	var err error
	for i := 0; i < cn.MaxRecoveryAttempts; i++ {
		time.Sleep(time.Duration(i*2) * time.Second)
		_, err = cn.connect()
		if err == nil {
			log.Println("reconnected")
			return nil
		}
	}
	return err
}

func (cn *Connector) publish(body []byte, routing_key string) error {
	ch, errc := cn.conn.Channel()
	if errc != nil {
		return errc
	}
	cn.channels[routing_key] = ch
	log.Println("got channel..")
	defer ch.Close()
	go func() {
		cn.chanErr <- <-ch.NotifyClose(make(chan *amqp.Error, 1))
	}()

	errch := cn.chanErrorHandling(ch, routing_key)
	if errch != nil {
		log.Println("Failed to recover channel")
		return errch
	}
	err := cn.channels[routing_key].Publish(
		EXCHANGE,    // exchange
		routing_key, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	)
	return err
}
func (cn *Connector) publication(routing_key string) error {

	go func() {
		cn.connErr <- <-cn.conn.NotifyClose(make(chan *amqp.Error)) //Listen to NotifyClose
		//cn.connErr <- &amqp.Error{Code: amqp.AccessRefused} //for test

	}()

	body := []byte("Hello, world!")

	for {
		errp := cn.publish(body, routing_key)
		if errp == nil {
			// Message published successfully, break out of the loop
			log.Println("published")
			break
		}
		errH := cn.connErrorHandling()
		if errH != nil {
			log.Println("Failed to publish message to RabbitMQ:")
			break
		}
	}

	return nil

}
func (cn *Connector) channelErrors(errC *amqp.Error) {
	log.Println("handling channel..")

	switch errC.Code {
	case amqp.AccessRefused:
		log.Println("access refused error, retrying...")
		// here might go mechanism to solver this error if it is possible to do with code

	case amqp.ChannelError:
		log.Println("channel error, retrying in 5 seconds...")

	default:
		log.Printf("unknown error code: %d, retrying in 1 second...", errC.Code)
	}
}
func (cn *Connector) connErrorHandling() error {
	res := make(chan error, 1)
	go func() {
		res <- cn.handleConnectionErr()
	}()
	select {
	case <-time.After(time.Duration(cn.ConnectionCloseTimeout) * time.Second):
		log.Panicln("timed out")
	case result := <-res:
		return result
	}
	return nil
}
func (cn *Connector) chanErrorHandling(ch *amqp.Channel, name string) error {
	res := make(chan error, 1)
	go func() {
		res <- cn.handleChannelErrors(ch, name)
	}()
	select {
	case <-time.After(time.Duration(cn.ConnectionCloseTimeout) * time.Second):
		//return errors.New("timed out")
		log.Panicln("timed out")
	case result := <-res:
		return result
	default:
		return nil
	}
	return nil
}
func (cn *Connector) handleConnectionErr() error {
	select {
	case err := <-cn.connErr:
		switch err.Code {
		case amqp.AccessRefused:
			log.Println("Access refused error:", err)
		case amqp.ConnectionForced:
			log.Println("Connection forced error:", err)
		default:
			log.Println("Unknown error:", err)
		}
		errR := cn.reconnect()
		if errR != nil {
			return errR
		}

	default:
		return nil
	}
	return nil
}
func (cn *Connector) handleChannelErrors(ch *amqp.Channel, name string) error {

	for {
		errC := <-cn.chanErr
		if errC != nil {
			log.Printf("Channel closed with error: %v, attempting to recover...", errC)
			cn.channelErrors(errC)
			var err error
			for i := 0; i < cn.MaxRecoveryAttempts; i++ {
				// Attempt to re-establish the channel
				//time.Sleep(time.Duration(cn.ConnectionCloseTimeout) * time.Second)//test
				ch, err = cn.conn.Channel()
				cn.channels[name] = ch
				if err == nil {
					log.Printf("Channel recovered after error: %v", err)
					return nil
				}
			}
			return err
		}
	}

}

func (cn *Connector) subsc(queue string) error {
	go func() {
		cn.connErr <- <-cn.conn.NotifyClose(make(chan *amqp.Error)) //Listen to NotifyClose
		//cn.connErr <- &amqp.Error{Code: amqp.AccessRefused} //for testing

	}()

	ch, errC := cn.conn.Channel()
	if errC != nil {
		return errC
	}
	log.Println("got channel..")
	cn.channels[queue] = ch
	var msgs <-chan amqp.Delivery
	var err error

	defer ch.Close()

	go func() {
		cn.chanErr <- <-cn.channels[queue].NotifyClose(make(chan *amqp.Error, 1))
		//cn.chanErr <- &amqp.Error{Code: amqp.AccessRefused} //for testing

	}()

	//cn.conn.Close() //for testing

	for {
		msgs, err = cn.channels[queue].Consume(
			queue, // queue
			"",    // consumer
			true,  // auto-ack
			false, // exclusive
			false, // no-local
			false, // no-wait
			nil,   // args
		)
		if err == nil {
			log.Println("consumed ")
			break
		}
		errH := cn.connErrorHandling()
		if errH != nil {
			log.Println("Failed to recover connection")
			return errH
		}
		errch := cn.chanErrorHandling(ch, queue)
		if errch != nil {
			log.Println("Failed to recover channel")
			return errch
		}
	}
	for msg := range msgs {
		log.Printf("Received message: %s", msg.Body)
	}

	return nil

}
func main() {
	connector := GetConnector()
	connector.connect()
	//
	//err := connector.publication(ROUTING_KEY)
	//if err != nil {
	//	log.Println(err)
	//}

	errs := connector.subsc(QUEUE_NAME)
	if errs != nil {
		log.Println(errs)
	}

}
