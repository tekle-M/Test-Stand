package main

import (
	"github.com/streadway/amqp"
	"log"
	"time"
	// "errors"
)

const (
	MAX_ATTEMPT = 5
	QUEUE_NAME =  "que"
	EXCHANGE = "new_ex"
	ROUTING_KEY = "rout"
)



type Connector struct {
	url      string
	conn     *amqp.Connection
	channels map[string]*amqp.Channel
	err chan *amqp.Error
}


func GetConnector() Connector {
	return Connector{url:"amqp://guest:guest@localhost:5672/", 
channels: map[string]*amqp.Channel{}, err : make(chan *amqp.Error,1)}
}

func (cn *Connector)connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(cn.url)
	if err != nil {
		return nil, err
	}
	cn.conn = conn
	go func() {
		cn.err<-<-cn.conn.NotifyClose(make(chan *amqp.Error)) //Listen to NotifyClose
		// <- errors.New("Connection Closed")
	}()
	log.Println("connected")
	return conn, nil
}

func (cn *Connector)reconnect() error{
	log.Println("reconnecting..")
	var err error
	for i := 0; i < MAX_ATTEMPT; i++ {
		time.Sleep(time.Duration(i*2)*time.Second)
		cn.conn,err = cn.connect()
		if err == nil {
			return nil
		}

	}
	return err
}

func (cn *Connector)channelErrors( errC *amqp.Error) {
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
func (cn *Connector)handleConnectionErr() error {
	select {
	case err := <-cn.err:
		log.Println(err.Code)
		switch err.Code {
		case amqp.AccessRefused:
			log.Println("Access refused error:", err)
			//I dont think this code could do anything in this case for example, and another errors too
			// maybe error should be solved externally as code cannt do anything if acces is refused for example
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
func (cn *Connector)publish(routing_key string) error {

	err := cn.handleConnectionErr()
	if err != nil {
		return err
	}
		
	ch, err := cn.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	log.Println("got channel..")


	body := []byte("Hello, world!")
	err = ch.Publish(
		EXCHANGE, // exchange
		routing_key,   // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	)
	if err != nil {
		log.Printf("err pub %v", err)

		return err
	}

	log.Printf("Sent message: %s", "body")
	
	return nil

}

func  (cn *Connector)subcribtion(queue string) error {
	select {
	case err := <-cn.err:
		log.Println(err)
		errR := cn.reconnect()
		if errR != nil {
			return errR
		}
	default:
	}
		
	ch, err := cn.conn.Channel()
	if err != nil {
		return err
	}
	log.Println("got channel..")
	cn.channels[queue]= ch

	defer ch.Close()
	closeCh := make(chan *amqp.Error, 1)
	ch.NotifyClose(closeCh)


	go cn.handleChannelErrors(closeCh, ch)

	msgs, err := ch.Consume(
		queue, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return err
	}

	for msg := range msgs {
		log.Printf("Received message: %s", msg.Body)
		closeCh <- &amqp.Error{Code: amqp.AccessRefused}
	}
	return nil
}

func  (cn *Connector)handleChannelErrors(chErr chan *amqp.Error,ch *amqp.Channel)  {
	for {
		errC := <-chErr
		if errC != nil {
			log.Printf("Channel closed with error: %v, attempting to recover...", errC)
			cn.channelErrors(errC)
			for {
				// Attempt to re-establish the channel
				var err error
				ch, err = cn.conn.Channel()
				if err == nil {
					log.Printf("Channel recovered after error: %v", err)
					// Re-register the error channel on the new channel
					ch.NotifyClose(chErr)
					break
				}
				time.Sleep(5 * time.Second) // Wait before retrying
			}
		}
	}
	
}



func main() {
	connector := GetConnector()
	connector.connect()
	
	err := connector.publish(ROUTING_KEY)
	if err != nil {
		println(err)
	}
	// errs := connector.subcribtion(QUEUE_NAME)
	// if errs != nil {
	// 	log.Println(errs)
	// }
	// log.Println("ok")
	

}
