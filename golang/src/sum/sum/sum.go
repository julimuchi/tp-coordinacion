package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue         middleware.Middleware
	outputExchange     middleware.Middleware
	clientFruitItemMap map[string]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		clientFruitItemMap: map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if envelope.Type == inner.MessageTypeEOF {
		if err := sum.handleEndOfRecordMessage(envelope.ClientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(envelope.Data, envelope.ClientId); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId string) error {
	slog.Info("Received End Of Records message")
	clientFruitsMap := sum.clientFruitItemMap[clientId]
	clientFruitsArr := make([]fruititem.FruitItem, 0, len(clientFruitsMap))
	for _, fruit := range clientFruitsMap {
		clientFruitsArr = append(clientFruitsArr, fruit)
	}

	if len(clientFruitsArr) > 0 {
		// No es necesario que los envelopes tengan distinto nombre
		// porque estan en bloques distintos...Pero por readibilidad :)
		dataEnvelope := inner.NewDataEnvelope(clientId, clientFruitsArr)
		dataMessage, err := inner.SerializeMessage(dataEnvelope)
		if err != nil {
			slog.Debug("While serializing Data message", "clientId", clientId, "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*dataMessage); err != nil {
			slog.Debug("While sending Data message", "clientId", clientId, "err", err)
			return err
		}
	}

	eofEnvelope := inner.NewEofEnvelope(clientId)
	eofMessage, err := inner.SerializeMessage(eofEnvelope)
	if err != nil {
		slog.Debug("While serializing EOF message", "clientId", clientId, "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "clientId", clientId, "err", err)
		return err
	}

	delete(sum.clientFruitItemMap, clientId)
	return nil
}

func (sum *Sum) handleDataMessage(fruitRecords []fruititem.FruitItem, clientId string) error {
	clientFruitsMap, ok := sum.clientFruitItemMap[clientId]
	if !ok {
		clientFruitsMap = map[string]fruititem.FruitItem{}
		sum.clientFruitItemMap[clientId] = clientFruitsMap
	}

	for _, fruitRecord := range fruitRecords {
		if existingFruitRecord, ok2 := clientFruitsMap[fruitRecord.Fruit]; ok2 {
			clientFruitsMap[fruitRecord.Fruit] = existingFruitRecord.Sum(fruitRecord)
		} else {
			clientFruitsMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
