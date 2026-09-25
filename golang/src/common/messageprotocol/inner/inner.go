package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/tkt"
)

func SerializeMessage(envelop Envelope) (*middleware.Message, error) {
	body, err := json.Marshal(envelop)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (*Envelope, error) {
	var envelope Envelope
	if err := json.Unmarshal([]byte(message.Body), &envelope); err != nil {
		return nil, err
	}
	if envelope.ClientId == "" {
		return nil, tkt.FormatError(ErrInvalidEnvelopeClientIdError)
	}
	if envelope.Type == "" {
		return nil, tkt.FormatError(ErrInvalidEnvelopetTypeError)
	}
	//TODO: podria validar el type perse

	return &envelope, nil

}
