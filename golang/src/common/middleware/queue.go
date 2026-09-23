package middleware

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	name         string
	connection   *amqp.Connection
	channel      *amqp.Channel
	consummerTag *string
}

func (q *Queue) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if err := q.channel.Qos(1, 0, false); err != nil {
		return checkRabbitConsumError(err)
	}
	consummerTag := buildConsummerTag(q.name)
	msgs, err := q.channel.Consume(
		q.name,       // queue
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
	q.consummerTag = &consummerTag

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

func (q *Queue) StopConsuming() error {
	if q.consummerTag == nil {
		// Si no se estaba consumiendo de la cola/exchange, no tiene efecto (asi lo pide la catedra)
		return nil
	}
	err := q.channel.Cancel(*q.consummerTag, false)
	if err != nil {
		return checkRabbitConsumError(err)
	}
	q.consummerTag = nil
	return nil
}

func (q *Queue) Send(msg Message) error {
	//TODO: Deberia validar empty msg?
	ctx := context.Background()
	err := q.channel.PublishWithContext(ctx,
		"",     // exchange
		q.name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		return checkRabbitSendError(err)
	}
	return nil
}

func (q *Queue) Close() error {
	if q.channel != nil {
		if err := q.channel.Close(); err != nil {
			return checkRabbitCloseError(err)
		}
	}
	if q.connection != nil {
		if err := q.connection.Close(); err != nil {
			return checkRabbitCloseError(err)
		}
	}
	return nil
}
