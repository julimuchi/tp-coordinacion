package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, err := rabbitConnect(&connectionSettings)
	if err != nil {
		return nil, err
	}
	ch, err := createDefaultRabbitChannel(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	// realmente no necesito guardarme una referencia a la cola de rabbit
	// con el nombre es suficiente
	_, err = createDefaultRabbitQueue(ch, queueName)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	queue := Queue{
		name:       queueName,
		connection: conn,
		channel:    ch,
	}
	return &queue, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, err := rabbitConnect(&connectionSettings)
	if err != nil {
		return nil, err
	}
	ch, err := createDefaultRabbitChannel(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	q, err := createAnonymousRabbitQueue(ch)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	for _, k := range keys {
		err := ch.QueueBind(
			q.Name,   // queue name
			k,        // routing key
			exchange, //exchange
			false,
			nil,
		)
		if err != nil {
			ch.Close()
			conn.Close()
			return nil, formatError(BIND_ERROR)
		}
	}

	exchangeObj := Exchange{
		exchangeName: exchange,
		queueName:    q.Name,
		channel:      ch,
		connection:   conn,
		keys:         keys,
	}

	return &exchangeObj, nil
}
