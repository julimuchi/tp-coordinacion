package tkt

import (
	"errors"
	"fmt"
)

func FormatError(errMessage string, msg ...string) error {
	if len(msg) == 0 {
		return errors.New(errMessage)
	}
	return fmt.Errorf("%s: %s", errMessage, msg)
}

func PanicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
