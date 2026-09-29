package join

import (
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue         middleware.Middleware
	outputQueue        middleware.Middleware
	clientFruits       map[string][]fruititem.FruitItem
	agreggationAmmount int
	eofClientCount     map[string]int
	topSize            int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:         inputQueue,
		outputQueue:        outputQueue,
		agreggationAmmount: config.AggregationAmount,
		clientFruits:       make(map[string][]fruititem.FruitItem),
		eofClientCount:     map[string]int{},
		topSize:            config.TopSize,
	}, nil
}

func (join *Join) Run() {
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)

	go func() {
		defer waitGroup.Done()
		err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			join.handleMessage(msg, ack, nack)
		})
		if err != nil {
			slog.Error("inputQueue stopped", "err", err)
		}
	}()

	common.HandleSignals()

	join.inputQueue.StopConsuming()

	waitGroup.Wait()

	join.closeOpenMiddlewares()
}

func (join *Join) closeOpenMiddlewares() {
	common.CloseMiddlewares([]middleware.Middleware{join.inputQueue, join.outputQueue})
}

func (aggregation *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
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

func (join *Join) handleDataMessage(fruitRecords []fruititem.FruitItem, clientId string) error {
	join.clientFruits[clientId] = append(join.clientFruits[clientId], fruitRecords...)
	return nil
}

func (join *Join) handleEndOfRecordsMessage(clientId string) error {
	join.eofClientCount[clientId]++
	if join.eofClientCount[clientId] < join.agreggationAmmount {
		return nil
	}
	// A partir de este punto recibió EOF de todos los nodos aggregation
	delete(join.eofClientCount, clientId)

	top := join.buildFruitTop(clientId)
	envelope := inner.NewDataEnvelope(clientId, top)

	if err := inner.SendEnvelope(join.outputQueue, envelope); err != nil {
		slog.Debug("While sending top message", "clientId", clientId, "err", err)
		return err
	}
	slog.Info("Sent final top", "clientId", clientId, "size", len(top))
	return nil
}

func (join *Join) buildFruitTop(clientId string) []fruititem.FruitItem {
	top := common.TopFruits(join.clientFruits[clientId], join.topSize)
	delete(join.clientFruits, clientId)
	return top
}
