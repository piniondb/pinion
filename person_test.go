package pinion_test

import (
	"fmt"
	"github.com/piniondb/pinion"
	"github.com/piniondb/store"
)

type nameType struct {
	last, middle, first string
}

// personType is a simple record containing information about an individual.
// It is used in various examples. Its fields do not need to be exported.
type personType struct {
	id   uint16
	name nameType
}

// MarshalBinary implements the encoding.BinaryMarshaler interface. It uses
// the piniondb/store package to pack fields efficiently. The order in which
// fields are packed needs to match the order used by UnmarshalBinary to unpack
// them.
func (p personType) MarshalBinary() (data []byte, err error) {
	var put store.PutBuffer
	put.Uint16(p.id)
	put.Str(p.name.last)
	put.Str(p.name.middle)
	put.Str(p.name.first)
	return put.Data()
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface. It uses
// a piniondb/store get buffer to extract the record's fields in the same order
// with which they were packed.
func (p *personType) UnmarshalBinary(data []byte) error {
	get := store.NewGetBuffer(data)
	get.Uint16(&p.id)
	get.Str(&p.name.last)
	get.Str(&p.name.middle)
	get.Str(&p.name.first)
	return get.Done()
}

// String satisfies the fmt.Stringer interface. This is not required by pinion
// but is almost always worthwhile for record presentation.
func (p personType) String() string {
	return fmt.Sprintf("%s %s %s / %d", p.name.first, p.name.middle, p.name.last, p.id)
}

// Name returns the name pinion will use to identify its collection of
// personType records.
func (p personType) Name() string {
	return "person"
}

// Indexes are enumerated here to facilitate record management on the part of
// the application. This is not required by pinion.
const (
	idxPersonID = iota
	idxPersonNameLast
	idxPersonNameFirst
	idxPersonCount
)

// Indexes are given names here to facilitate reporting. This is not required
// by pinion.
var personIndexNames = []string{"ID", "Last name", "First name"}

// IndexCount returns the number of indexes pinion should maintain for records
// of personType.
func (p personType) IndexCount() uint8 {
	return idxPersonCount
}

// New returns a pointer to a variable of type personType. pinion will use this
// as a working buffer for key management.
func (p personType) New() pinion.Record {
	return new(personType)
}

// NextID receives an autoincremented ID from pinion when a record is added.
// The application is free to use a storage type that is different than uint64,
// however it must be aware that an autoincremented ID is consumed for each
// added record and are not reused for records that are deleted.
func (p *personType) NextID(id uint64) {
	p.id = uint16(id)
}

// Key returns the key associated with the receiver for the index specified by
// idx. A piniondb/store key buffer is used to make sure all key fields are of
// fixed width and properly sortable. Only the primary key (that is, the one
// associated with index 0) needs to be unique for a given record type.
// Internally, pinion appends the primary key to the other keys.
func (p personType) Key(idx uint8) (key []byte, err error) {
	var kb store.KeyBuffer
	switch idx {
	case idxPersonID:
		kb.Uint16(p.id)
	case idxPersonNameLast:
		kb.Str(p.name.last, 12)
		kb.Str(p.name.first, 8)
		kb.Str(p.name.middle, 1)
	case idxPersonNameFirst:
		kb.Str(p.name.first, 8)
		kb.Str(p.name.middle, 1)
		kb.Str(p.name.last, 12)
	default:
		kb.SetError(fmt.Errorf("index %d is out of bounds", idx))
	}
	return kb.Data()
}
