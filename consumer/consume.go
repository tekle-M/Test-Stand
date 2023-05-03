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

type Consumer struct {
	url                    string
	conn                   *amqp.Connection
	mutex                  *sync.Mutex
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
		url:                    url,
		conn:                   conn,
		mutex:                  &sync.Mutex{},
		channels:               make(map[string]*amqp.Channel),
		ConnectionCloseTimeout: 5,
		MaxRecoveryAttempts:    3,
	}, nil
}
func (cns *Consumer) subs(queueName string, msgs <-chan amqp.Delivery, connErr chan *amqp.Error, wg *sync.WaitGroup) {
	for i := 0; i < cns.MaxRecoveryAttempts; i++ {
		log.Printf("attempt : %v", i+1)

		select {
		case errRes := <-connErr:
			errH := cns.connErrorHandling(errRes, queueName)
			if errH != nil {
				log.Panicln("Failed to publish message to RabbitMQ:")
			}
			log.Println("handled errors ")

		default:
		}

		var consErr error
		msgs, consErr = cns.channels[queueName].Consume(
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
			log.Println("Consumed")
			wg.Add(1)
			break
		}
		log.Printf("error Consuming", connErr)

	}
}

func (cns *Consumer) Consume(queueName string) error {
	cns.mutex.Lock()

	ch, err := cns.conn.Channel()
	if err != nil {
		log.Println("cannot open channel")
		return err
	}
	cns.channels[queueName] = ch
	cns.mutex.Unlock()

	log.Println("got channel")

	connErr := make(chan *amqp.Error, 1)
	//chanErr := make(chan *amqp.Error, 1)

	go func(cn chan *amqp.Error) {
		log.Println("in cON")

		//cn <- <-pb.conn.NotifyClose(make(chan *amqp.Error, 1)) //Listen to NotifyClose
		cn <- &amqp.Error{Code: amqp.AccessRefused} //for test

	}(connErr)
	var wg sync.WaitGroup
	//cns.conn.Close()
	var msgs <-chan amqp.Delivery
	cns.conn.Close() //for test

	for i := 0; i < cns.MaxRecoveryAttempts; i++ {
		log.Printf("attempt : %v", i+1)

		select {
		case errRes := <-connErr:
			errH := cns.connErrorHandling(errRes, queueName)
			if errH != nil {
				log.Panicln("Failed to publish message to RabbitMQ:")
			}
			log.Println("handled errors ")

		default:
		}

		var consErr error
		msgs, consErr = cns.channels[queueName].Consume(
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
			log.Println("Consumed")
			wg.Add(1)
			break
		}
		log.Printf("error Consuming", connErr)

	}
	//cns.conn.Close()//for test

	go func() {
		defer wg.Done()
		for {
			if !cns.conn.IsClosed() && msgs != nil {
				log.Println("in default")

				for d := range msgs {
					log.Printf("receive delivery: %s", d.Body)
				}
				break

			} else {
				cns.subs(queueName, msgs, connErr, &wg)
				continue
			}
		}
	}()
	wg.Wait()
	log.Println("done")
	return nil
}
func (cns *Consumer) Close() error {
	return cns.conn.Close()
}

func (cns *Consumer) connErrorHandling(ch *amqp.Error, name string) error {
	res := make(chan error, 1)
	go func() {
		res <- cns.handleConnectionErr(ch, name)
	}()
	select {
	case <-time.After(time.Duration(cns.ConnectionCloseTimeout) * time.Second):
		log.Panicln("timed out")
	case result := <-res:
		return result
	}
	return nil
}

func (cns *Consumer) handleConnectionErr(connErr *amqp.Error, name string) error {
	switch connErr.Code {
	case amqp.AccessRefused:
		log.Println("Access refused error:", connErr)
	case amqp.ConnectionForced:
		log.Println("Connection forced error:", connErr)
	default:
		log.Println("Unknown error:", connErr)
	}
	errR := cns.reconnect()
	if errR != nil {
		return errR
	}
	chann, err := cns.conn.Channel()
	if err != nil {
		return err
	}
	cns.mutex.Lock()
	defer cns.mutex.Unlock()
	cns.channels[name] = chann
	log.Println("Channel recovered")
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

	err := connector.Consume(QUEUE_NAME)
	if err != nil {
		log.Println(err)
	}

}
