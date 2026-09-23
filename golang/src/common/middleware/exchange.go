package middleware

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	exchangeName string
	connection   *amqp.Connection
	channel      *amqp.Channel
	consummerTag *string
	queueName    string
	keys         []string
}

func (e *Exchange) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if err := e.channel.Qos(1, 0, false); err != nil {
		return checkRabbitConsumError(err)
	}
	consummerTag := buildConsummerTag(e.queueName)
	msgs, err := e.channel.Consume(
		e.queueName,  // queue
		consummerTag, // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return checkRabbitConsumError(err)
	}
	e.consummerTag = &consummerTag

	for d := range msgs {
		callbackFunc(
			Message{
				Body: string(d.Body),
			},
			func() {
				d.Ack(
					false, // por mensaje
				)
			},
			func() {
				d.Nack(
					false, //por mensaje
					true,  //lo reencolo
				)
			},
		)
	}

	return nil
}

func (e *Exchange) StopConsuming() error {
	if e.consummerTag == nil {
		// Si no se estaba consumiendo de la cola/exchange, no tiene efecto (asi lo pide la catedra)
		return nil
	}
	err := e.channel.Cancel(*e.consummerTag, false)
	if err != nil {
		return checkRabbitConsumError(err)
	}
	e.consummerTag = nil
	return nil
}

func (e *Exchange) Send(msg Message) error {
	//TODO: Deberia validar empty msg?
	ctx := context.Background()
	for _, key := range e.keys {
		err := e.channel.PublishWithContext(ctx,
			e.exchangeName, // exchange
			key,            // routing key
			false,          // mandatory
			false,          // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			return checkRabbitSendError(err)
		}
	}

	return nil
}

func (e *Exchange) Close() error {
	if e.channel != nil {
		if err := e.channel.Close(); err != nil {
			return checkRabbitCloseError(err)
		}
	}
	if e.connection != nil {
		if err := e.connection.Close(); err != nil {
			return checkRabbitCloseError(err)
		}
	}
	return nil
}
