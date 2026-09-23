package middleware

import (
	"errors"
	"fmt"
)

const (
	EMPTY_CONN_HOST_ERROR      = "Empty connection host"
	EMPTY_CONN_PORT_ERROR      = "Empty connection port"
	CONN_ERROR                 = "An error happend connecting to rabbit"
	CHANNEL_CREATE_ERROR       = "An error happend connecting a channel on rabbit"
	QUEUE_CREATE_ERROR         = "An error happend creating a queue on rabbit"
	UNKNOWN_ERROR              = "An unknown error connecting"
	UNKNOWN_CONSUMER_TAG_ERROR = "unknown queue consumer tag"
	BIND_ERROR                 = "An error connecting bing a que on rabbit"
)

func formatError(errMessage string, msg ...string) error {
	if len(msg) == 0 {
		return errors.New(errMessage)
	}
	return fmt.Errorf("%s: %s", errMessage, msg)
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
