package glfs

import "fmt"

type ErrNoEnt struct {
	Name string
}

func (e ErrNoEnt) Error() string {
	return fmt.Sprintf("no entry at %s", e.Name)
}

type ErrRefType struct {
	Have, Want Type
}

func (e ErrRefType) Error() string {
	return fmt.Sprintf("wrong type HAVE: %v WANT: %v", e.Have, e.Want)
}
