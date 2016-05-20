package pinion_test

// Simple data structure to test best-case performance. No secondary index, no
// meaningful or extra fields.

import (
	"fmt"
	"github.com/piniondb/pinion"
	"github.com/piniondb/store"
)

type intType struct {
	id uint32
}

func (i intType) MarshalBinary() (data []byte, err error) {
	var put store.PutBuffer
	put.Uint32(i.id)
	return put.Data()
}

func (i *intType) UnmarshalBinary(data []byte) error {
	get := store.NewGetBuffer(data)
	get.Uint32(&i.id)
	return get.Done()
}

func (i intType) String() string {
	return fmt.Sprintf("%s", intStr(i.id))
}

func (i intType) Name() string {
	return "int"
}

const (
	idxIntID = iota
	idxIntCount
)

func (i intType) IndexCount() uint8 {
	return idxIntCount
}

func (i intType) New() pinion.Record {
	return new(intType)
}

func (i *intType) NextID(id uint64) {
	i.id = uint32(id)
}

func (i intType) Key(idx uint8) (key []byte, err error) {
	var kb store.KeyBuffer
	if idx == idxIntID {
		kb.Uint32(i.id)
	} else {
		kb.SetError(fmt.Errorf("index %d is out of bounds", idx))
	}
	return kb.Data()
}
