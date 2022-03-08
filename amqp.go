package amqp

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	connection "github.com/serqol/go-pool"
	"github.com/streadway/amqp"
)

type Amqp struct {
	connection         *amqp.Connection
	channel            *amqp.Channel
	configuration      *Configuration
	confirmChannel     chan amqp.Confirmation
	pendingMessages    map[uint64]*amqp.Publishing
	currentDeliveryTag uint64
}

type Configuration struct {
	host         string
	port         string
	user         string
	password     string
	exchange     string
	exchangeType string
	queue        string
	durable      bool
	confirm      bool
}

func GetConfiguration(host string, port string, user string, password string, exchange string, exchangeType string, queue string, durable bool, confirm bool) *Configuration {
	return &Configuration{
		host:         host,
		port:         port,
		user:         user,
		password:     password,
		exchange:     exchange,
		exchangeType: exchangeType,
		queue:        queue,
		durable:      durable,
		confirm:      confirm,
	}
}

func GetConnector(configuration *Configuration, minActive uint8, maxActive uint16, maxIdle uint16) *connection.Connector {
	return connection.GetConnector(
		connectorFunction(configuration),
		closeFunction,
		minActive,
		maxActive,
		maxIdle,
	)
}

func connectorFunction(configuration *Configuration) func() (interface{}, error) {
	return func() (interface{}, error) {
		return Publisher(configuration)
	}
}

func closeFunction(connection interface{}) { connection.(*Amqp).Close() }

func (instance *Amqp) Publish(body []byte) error {
	exchange, queue := instance.configuration.exchange, instance.configuration.queue
	publishMessage := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/json",
		ContentEncoding: "",
		Body:            body,
		DeliveryMode:    amqp.Persistent,
		Priority:        0,
	}
	if err := instance.channel.Publish(
		exchange,
		queue,
		false,
		false,
		publishMessage,
	); err != nil {
		return err
	}
	if instance.configuration.confirm {
		currentDeliveryTag := atomic.LoadUint64(&instance.currentDeliveryTag)
		atomic.AddUint64(&instance.currentDeliveryTag, 1)
		instance.pendingMessages[currentDeliveryTag+1] = &publishMessage
		instance.confirmOne(instance.confirmChannel)
	}
	return nil
}

func Consumer(configuration map[string]string) *Amqp {
	return &Amqp{} // todo: implement
}

func (instance *Amqp) Close() {
	instance.channel.Close()
	instance.connection.Close()
}

func Publisher(configuration *Configuration) (*Amqp, error) {
	host, user, password, port := configuration.host, configuration.user, configuration.password, configuration.port
	connStr := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, password, host, port)
	connection, err := amqp.Dial(connStr)
	if err != nil {
		return nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}
	exchangeDeclare(channel, configuration)
	queueDeclare(channel, configuration)
	bind(channel, configuration)
	var confirms chan amqp.Confirmation
	if configuration.confirm {
		if err := channel.Confirm(false); err != nil {
			return nil, err
		}
		confirmationChannel := make(chan amqp.Confirmation, 300)
		confirms = channel.NotifyPublish(confirmationChannel)
	}

	return &Amqp{connection, channel, configuration, confirms, map[uint64]*amqp.Publishing{}, 0}, nil
}

func exchangeDeclare(channel *amqp.Channel, configuration *Configuration) {
	if err := channel.ExchangeDeclare(
		configuration.exchange,     // name
		configuration.exchangeType, // type
		true,                       // durable
		false,                      // auto-deleted
		false,                      // internal
		false,                      // noWait
		nil,                        // arguments
	); err != nil {
		log.Fatal(err)
	}
}

func queueDeclare(channel *amqp.Channel, configuration *Configuration) {
	_, err := channel.QueueDeclare(configuration.queue, configuration.durable, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func bind(channel *amqp.Channel, configuration *Configuration) {
	channel.QueueBind(configuration.queue, configuration.queue, configuration.exchange, false, nil)
}

func (instance *Amqp) confirmOne(confirms <-chan amqp.Confirmation) {
	select {
	case confirmed := <-confirms:
		if confirmed.Ack {
			delete(instance.pendingMessages, confirmed.DeliveryTag)
			fmt.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
			return
		} else {
			undeliveredMessage := instance.pendingMessages[confirmed.DeliveryTag]
			_ = undeliveredMessage // do something with undelived message
			fmt.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
		}
	case <-time.After(5 * time.Millisecond):
		fmt.Print("No more time to wait for confirmation, return")
	}
}
