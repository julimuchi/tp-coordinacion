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
	inputGatewayQueue      middleware.Middleware
	outputAggExchangesList []middleware.Middleware
	outputSumExchange      middleware.Middleware
	inputSumExchange       middleware.Middleware
	clientFruitItemMap     map[string]map[string]fruititem.FruitItem
	mutex                  sync.Mutex
	config                 SumConfig
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}
	openMiddlewares := make([]middleware.Middleware, 0)
	inputGatewayQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	openMiddlewares = append(openMiddlewares, inputGatewayQueue)

	outputAggExchangeList, err := createAggregationExchangeList(config.AggregationPrefix, config.AggregationAmount, connSettings)
	openMiddlewares = append(openMiddlewares, outputAggExchangeList...)
	if err != nil {
		common.CloseMiddlewares(openMiddlewares)
		return nil, err
	}

	sumExchangeRouteKeys := common.BuildExchangeRouteKeys(config.SumAmount, config.SumPrefix)
	outputSumExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, sumExchangeRouteKeys, connSettings)
	if err != nil {
		common.CloseMiddlewares(openMiddlewares)
		return nil, err
	}
	openMiddlewares = append(openMiddlewares, outputSumExchange)

	inputSumExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}, connSettings)
	if err != nil {
		common.CloseMiddlewares(openMiddlewares)
		return nil, err
	}

	return &Sum{
		inputGatewayQueue:      inputGatewayQueue,
		outputAggExchangesList: outputAggExchangeList,
		clientFruitItemMap:     map[string]map[string]fruititem.FruitItem{},
		outputSumExchange:      outputSumExchange,
		inputSumExchange:       inputSumExchange,
		config:                 config,
	}, nil
}

func createAggregationExchangeList(aggPrefix string, aggAmount int, connSetting middleware.ConnSettings) ([]middleware.Middleware, error) {
	list := make([]middleware.Middleware, 0, aggAmount)
	routeKeys := common.BuildExchangeRouteKeys(aggAmount, aggPrefix)

	for _, key := range routeKeys {
		m, err := middleware.CreateExchangeMiddleware(aggPrefix, []string{key}, connSetting)
		if err != nil {
			return list, err
		}
		list = append(list, m)
	}

	return list, nil
}

func (sum *Sum) Run() {
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		err := sum.inputSumExchange.StartConsuming(
			func(msg middleware.Message, ack, nack func()) {
				sum.handleSumMessage(msg, ack, nack)
			},
		)
		if err != nil {
			slog.Error("inputSumExchange stopped", "err", err)
		}
	}()
	go func() {
		defer waitGroup.Done()
		err := sum.inputGatewayQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleGatewayMessage(msg, ack, nack)
		})
		if err != nil {
			slog.Error("inputGatewayQueue stopped", "err", err)
		}
	}()

	common.HandleSignals()

	sum.inputGatewayQueue.StopConsuming()
	sum.inputSumExchange.StopConsuming()

	waitGroup.Wait()
	sum.closeOpenMiddlewares()
}

func (sum *Sum) closeOpenMiddlewares() {
	totalOpen := 3 + sum.config.AggregationAmount
	openMiddlewares := make([]middleware.Middleware, 0, totalOpen)
	openMiddlewares = append(openMiddlewares, sum.inputGatewayQueue)

	openMiddlewares = append(openMiddlewares, sum.inputGatewayQueue, sum.outputSumExchange, sum.inputSumExchange)
	openMiddlewares = append(openMiddlewares, sum.outputAggExchangesList...)
	common.CloseMiddlewares(openMiddlewares)
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

func (sum *Sum) handleBroadcastEofMessage(clientId string) error {
	slog.Info("sharing EOF message between sum nodes", "clientId", clientId)
	envelope := inner.NewBroadcastEofEnvelope(clientId)
	return inner.SendEnvelope(sum.outputSumExchange, envelope)
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
		if err := sum.handleBroadcastEofMessage(envelope.ClientId); err != nil {
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

	if err := sum.distributeAndSend(clientId); err != nil {
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

func (sum *Sum) distributeAndSend(clientId string) error {
	fruitsSets := common.SplitFruitsByAggregator(sum.clientFruitItemMap[clientId], sum.config.AggregationAmount)

	for aggBoxId, fruits := range fruitsSets {
		if len(fruits) == 0 {
			continue
		}
		outputAggBox := sum.outputAggExchangesList[aggBoxId]
		dataEnvelope := inner.NewDataEnvelope(clientId, fruits)
		if err := inner.SendEnvelope(outputAggBox, dataEnvelope); err != nil {
			return err
		}
	}

	for _, outputAggBox := range sum.outputAggExchangesList {
		eofEnvelope := inner.NewEofEnvelope(clientId)
		if err := inner.SendEnvelope(outputAggBox, eofEnvelope); err != nil {
			return err
		}
	}

	return nil
}
