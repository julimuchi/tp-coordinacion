package messagehandler

import (
	"github.com/google/uuid"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	ClientId string
}

func NewMessageHandler() MessageHandler {
	clientId := uuid.NewString()
	return MessageHandler{
		ClientId: clientId,
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	envelope := inner.NewDataEnvelope(
		messageHandler.ClientId,
		[]fruititem.FruitItem{fruitRecord},
	)
	return inner.SerializeMessage(envelope)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	envelope := inner.NewEofEnvelope(
		messageHandler.ClientId,
	)
	return inner.SerializeMessage(envelope)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	envelope, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	//TODO: validaciones sobre el envelope?

	if envelope.ClientId != messageHandler.ClientId {
		return nil, nil
	}
	return envelope.Data, nil

}
