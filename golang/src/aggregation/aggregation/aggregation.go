package aggregation

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue        middleware.Middleware
	inputExchange      middleware.Middleware
	clientFruitItemMap map[string]map[string]fruititem.FruitItem
	eofClientCount     map[string]int
	topSize            int
	sumAmount          int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:        outputQueue,
		inputExchange:      inputExchange,
		clientFruitItemMap: map[string]map[string]fruititem.FruitItem{},
		topSize:            config.TopSize,
		sumAmount:          config.SumAmount,
		eofClientCount:     map[string]int{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)

	go func() {
		defer waitGroup.Done()
		aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			aggregation.handleMessage(msg, ack, nack)
		})
	}()

	common.HandleSignals()

	aggregation.inputExchange.StopConsuming()

	waitGroup.Wait()

	aggregation.closeOpenMiddlewares()
}

func (aggregation *Aggregation) closeOpenMiddlewares() {
	common.CloseMiddlewares([]middleware.Middleware{aggregation.inputExchange, aggregation.outputQueue})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if envelope.Type == inner.MessageTypeEOF {
		if err := aggregation.handleEndOfRecordsMessage(envelope.ClientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := aggregation.handleDataMessage(envelope.Data, envelope.ClientId); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string) error {
	aggregation.eofClientCount[clientId]++
	if aggregation.eofClientCount[clientId] < aggregation.sumAmount {
		return nil
	}
	// A partir de este punto recibio EOF de todos los nodos sum
	delete(aggregation.eofClientCount, clientId)

	fruitTopRecords := aggregation.buildFruitTop(clientId)
	if len(fruitTopRecords) > 0 {
		// No es necesario que los envelopes tengan distinto nombre
		// porque estan en bloques distintos...Pero por readibilidad :)
		dataEnvelope := inner.NewDataEnvelope(clientId, fruitTopRecords)
		if err := inner.SendEnvelope(aggregation.outputQueue, dataEnvelope); err != nil {
			slog.Debug("While sending Data message", "clientId", clientId, "err", err)
			return err
		}
	}

	eofEnvelope := inner.NewEofEnvelope(clientId)
	if err := inner.SendEnvelope(aggregation.outputQueue, eofEnvelope); err != nil {
		slog.Debug("While sending EOF message", "clientId", clientId, "err", err)
		return err
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(fruitRecords []fruititem.FruitItem, clientId string) error {
	clientFruitsMap, ok := aggregation.clientFruitItemMap[clientId]
	if !ok {
		clientFruitsMap = map[string]fruititem.FruitItem{}
		aggregation.clientFruitItemMap[clientId] = clientFruitsMap
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

func (aggregation *Aggregation) buildFruitTop(clientId string) []fruititem.FruitItem {
	slog.Info("building fruit top", "clientId", clientId)
	clientFruitsMap := aggregation.clientFruitItemMap[clientId]
	fruitItems := make([]fruititem.FruitItem, 0, len(clientFruitsMap))
	for _, fruit := range clientFruitsMap {
		fruitItems = append(fruitItems, fruit)
	}

	delete(aggregation.clientFruitItemMap, clientId)
	return common.TopFruits(fruitItems, aggregation.topSize)
}
