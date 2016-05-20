package pinion_test

import (
	"fmt"
	"github.com/piniondb/pinion"
	"github.com/piniondb/store"
	"github.com/piniondb/str"
)

// quantityType stores an unsigned integer along with its English word
// equivalent. This is convenient for database index tests because the records
// can be programmatically generated and the fields sort differently.
type quantityType struct {
	id  uint32
	val []uint8
}

func (q quantityType) MarshalBinary() (data []byte, err error) {
	var put store.PutBuffer
	put.Uint32(q.id)
	put.Bytes(q.val)
	return put.Data()
}

func (q *quantityType) UnmarshalBinary(data []byte) error {
	get := store.NewGetBuffer(data)
	get.Uint32(&q.id)
	get.Bytes(&q.val)
	return get.Done()
}

func intStr(val uint32) string {
	return str.Delimit(fmt.Sprintf("%d", val), ",", 3)
}

func (q quantityType) String() string {
	return fmt.Sprintf("[%11s : %s]", intStr(q.id), str.QuantityDecode(q.val))
}

func (q quantityType) Name() string {
	return "quantity"
}

const (
	idxQuantityID = iota
	idxQuantityVal
	idxQuantityCount
)

var quantityIndexNames = []string{"ID", "English"}

func (q quantityType) IndexCount() uint8 {
	return idxQuantityCount
}

func (q quantityType) New() pinion.Record {
	return new(quantityType)
}

func (q *quantityType) NextID(id uint64) {
	// Unique IDs are managed in application so there is nothing to do here
}

func (q quantityType) Key(idx uint8) (key []byte, err error) {
	var kb store.KeyBuffer
	switch idx {
	case idxQuantityID:
		kb.Uint32(q.id)
	case idxQuantityVal:
		kb.Bytes(q.val, 12)
	default:
		kb.SetError(fmt.Errorf("index %d is out of bounds", idx))
	}
	return kb.Data()
}
