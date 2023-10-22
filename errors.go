package glfs

import (
	"errors"
	"fmt"
)

type ErrNoEnt struct {
	Name string
}

func (e ErrNoEnt) Error() string {
	return fmt.Sprintf("no entry at %s", e.Name)
}

func IsErrNoEnt(err error) bool {
	return errors.As(err, new(ErrNoEnt))
}

type ErrRefType struct {
	Have, Want Type
}

func (e ErrRefType) Error() string {
	return fmt.Sprintf("wrong type HAVE: %v WANT: %v", e.Have, e.Want)
}
