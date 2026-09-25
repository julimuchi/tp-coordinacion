package sum

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common"
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
	inputGatewayQueue         middleware.Middleware
	outputAggregationExchange middleware.Middleware
	outputSumExchange         middleware.Middleware
	inputSumExchange          middleware.Middleware
	clientFruitItemMap        map[string]map[string]fruititem.FruitItem
	mutex                     sync.Mutex
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputGatewayQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	aggregationExchangeRouteKeys := common.BuildExchangeRouteKeys(config.AggregationAmount, config.AggregationPrefix)
	outputAggregationExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, aggregationExchangeRouteKeys, connSettings)
	if err != nil {
		inputGatewayQueue.Close()
		return nil, err
	}

	sumExchangeRouteKeys := common.BuildExchangeRouteKeys(config.SumAmount, config.SumPrefix)
	outputSumExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, sumExchangeRouteKeys, connSettings)
	if err != nil {
		inputGatewayQueue.Close()
		outputAggregationExchange.Close()
		return nil, err
	}

	inputSumExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}, connSettings)
	if err != nil {
		inputGatewayQueue.Close()
		outputAggregationExchange.Close()
		outputSumExchange.Close()
		return nil, err
	}
	return &Sum{
		inputGatewayQueue:         inputGatewayQueue,
		outputAggregationExchange: outputAggregationExchange,
		clientFruitItemMap:        map[string]map[string]fruititem.FruitItem{},
		outputSumExchange:         outputSumExchange,
		inputSumExchange:          inputSumExchange,
	}, nil
}

func (sum *Sum) Run() {
	go func() {
		err := sum.inputSumExchange.StartConsuming(
			func(msg middleware.Message, ack, nack func()) {
				sum.handleSumMessage(msg, ack, nack)
			},
		)
		if err != nil {
			slog.Error("inputSumExchange stopped", "err", err)
		}
	}()

	err := sum.inputGatewayQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleGatewayMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("inputGatewayQueue stopped", "err", err)
	}
}

func (sum *Sum) handleSumMessage(msg middleware.Message, ack func(), nack func()) {
	//TODO: tengo alguna duda si lockear aca es overkill
	// y deberia lockear en cada funcion especifica...
	sum.mutex.Lock()
	defer sum.mutex.Unlock()
	defer ack()

	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if envelope.Type != inner.MessageTypeBroadcastEOF {
		slog.Info("Wrong message type for sum exchange", "type", envelope.Type)
		return
	}

	if err := sum.handleEndOfRecordMessage(envelope.ClientId); err != nil {
		slog.Error("While handleing ", "err", err)
	}

}

func (sum *Sum) broadcastEof(clientId string) error {
	slog.Info("sharing EOF message between sum nodes", "clientId", clientId)
	envelope := inner.NewBroadcastEofEnvelope(clientId)
	message, err := inner.SerializeMessage(envelope)
	if err != nil {
		slog.Debug("While serializing Data message", "clientId", clientId, "err", err)
		return err
	}
	if err := sum.outputSumExchange.Send(*message); err != nil {
		slog.Debug("While sending Data message", "clientId", clientId, "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleGatewayMessage(msg middleware.Message, ack func(), nack func()) {
	//TODO: tengo alguna duda si lockear aca es overkill
	// y deberia lockear en cada funcion especifica...
	sum.mutex.Lock()
	defer sum.mutex.Unlock()
	defer ack()

	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch envelope.Type {
	case inner.MessageTypeEOF:
		if err := sum.broadcastEof(envelope.ClientId); err != nil {
			slog.Error("While broadcasting end of record message", "err", err)
		}
	case inner.MessageTypeData:
		if err := sum.handleDataMessage(envelope.Data, envelope.ClientId); err != nil {
			slog.Error("While handling data message", "err", err)
		}
	default:
		// TODO: Quiza deveria validarlo antes
		slog.Error("Unknow envelope messageType")
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId string) error {

	slog.Info("Received End Of Records message", "clientId", clientId)
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
		if err := sum.outputAggregationExchange.Send(*dataMessage); err != nil {
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
	if err := sum.outputAggregationExchange.Send(*eofMessage); err != nil {
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
