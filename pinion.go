/*
 * Copyright (c) 2016 Kurt Jung (Gmail: piniondb)
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package pinion

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/boltdb/bolt"
)

var (
	// ErrMissingIndex is reported when data is to be accessed and the application
	// record indicates that no indexes are present
	ErrMissingIndex = errors.New("at least one index must be defined")
	// ErrMissingRecord indicates a corrupt database in which data is not
	// associated with a valid primary key
	ErrMissingRecord = errors.New("missing record for valid index")
	// ErrNotOpen is reported when a database operation is attempted on a closed
	// database instance
	ErrNotOpen = errors.New("database is not open")
	// ErrRecNotFound is reported when no match is found for the requested record
	ErrRecNotFound = errors.New("record not found")
)

const (
	// Version identifies the database compatiblity level
	Version = 1
	// The following loop limit dictates the maximum number of change operations
	// that can take place in an writeable transaction. It is empirically
	// determined. If the limit is too low, the high cost of obtaining buckets and
	// committing becomes a large factor. If the limit is too high, performance
	// degrades because the uncommitted bolt pages become congested.
	// Benchmark for 50,000 records:
	// 100,000: 9.8 s
	//  25,000: 2.9 s
	//  15,000: 2.3 s
	//  12,500: 1.9 s
	//  10,000: 2.1 s
	//   5,000: 2.3 s
	//   2,500: 3.8 s
	//   1,000: 8.6 s
	cnLoopCount = 12500
)

// The Record interface specifies methods that allow pinion to manage multiply
// indexed records. The Name() and IndexCount() methods return constants; they
// return information about the type of the method receiver rather than the
// receiver instance itself. They are included here to allow all the
// information needed for pinion to manage database operations to be specified
// in one location.
type Record interface {
	// Convert the record identified by the method receiver to a byte sequence.
	encoding.BinaryMarshaler
	// Convert the specified byte sequence to the record identified by the method
	// receiver.
	encoding.BinaryUnmarshaler
	// Return the number of indexes associated with the record receiver. This
	// value must remain constant.
	IndexCount() uint8
	// Name of record for database table (invariant).
	Name() string
	// Construct a key for the index specified by idx.
	Key(idx uint8) (key []byte, err error)
	// Provide a temporary buffer for internal use. The returned record should be
	// of the same type as the method receiver.
	New() Record
	// Receive an autoincremented ID prior to inserting a record. This is called
	// only when the application calls Add(), not Put(). It is called before calls
	// are made to Key().
	NextID(uint64)
}

// The DB type manages data access with an underlying bolt database. It is safe
// for concurrent goroutine use. Only one instance of this type should be
// active at a time.
type DB struct {
	boltDB *bolt.DB
	opt    Options
}

// The Options type is used to configure the database when it is opened.
type Options struct {
	BoltOpt bolt.Options
	// Consider flag to control whether primary key is concatenated to other keys
}

// bucketGrpType holds all buckets that store data and indexes for a record
// type
type bucketGrpType struct {
	rec  *bolt.Bucket
	idxs []*bolt.Bucket
}

// valType holds a record's data and keys
type valType struct {
	data []byte
	keys [][]byte
}

// bucket returns the named bucket. It is valid for the duration of the
// specified transaction. If createIfNeeded is true, the bucket will be created
// if it does not already exist. The transaction must allow writing if
// createIfNeeded is true.
func bucket(tx *bolt.Tx, keyStr string, createIfNeeded bool) (bck *bolt.Bucket, err error) {
	key := []byte(keyStr)
	if createIfNeeded {
		bck, err = tx.CreateBucketIfNotExists(key)
	} else {
		bck = tx.Bucket(key)
		if bck == nil {
			err = fmt.Errorf("bucket \"%s\" missing", keyStr)
		}
	}
	return
}

// subbucket returns the bucket specified by idx. It is valid for the duration
// of the current transaction. If createIfNeeded is true, the bucket will be
// created if it does not already exist. The current transaction must allow
// writing if createIfNeeded is true.
func subbucket(parent *bolt.Bucket, parentNameStr string, idx uint8, createIfNeeded bool) (bck *bolt.Bucket, err error) {
	var key [1]byte
	key[0] = idx
	if createIfNeeded {
		bck, err = parent.CreateBucketIfNotExists(key[:])
	} else {
		bck = parent.Bucket(key[:])
		if bck == nil {
			err = fmt.Errorf("subbucket %s/%d missing", parentNameStr, idx)
		}
	}
	return
}

// bucketGet retrieves a record's storage buckets. If createIfNeeded is set,
// the buckets will be created if they do not already exist. The transaction
// must allow writing if createIfNeeded is true.
func bucketGet(recPtr Record, count uint8, createIfNeeded bool, tx *bolt.Tx) (bck bucketGrpType, err error) {
	if count > 0 {
		nameStr := recPtr.Name()
		bck.rec, err = bucket(tx, nameStr, createIfNeeded)
		if err == nil {
			bck.idxs = make([]*bolt.Bucket, count)
			for j := uint8(0); j < count && err == nil; j++ {
				bck.idxs[j], err = subbucket(bck.rec, nameStr, j, createIfNeeded)
			}
		}
	} else {
		err = ErrMissingIndex
	}
	return
}

// currentGet retrieves from the database the record and keys associated with
// primaryKey.
func (bck bucketGrpType) currentGet(recPtr Record, count uint8, primaryKey []byte) (val valType, err error) {
	var j uint8
	val.data = bck.idxs[0].Get(primaryKey)
	if val.data != nil {
		recPtr.UnmarshalBinary(val.data)
		val.keys = make([][]byte, count)
		for j = 0; j < count && err == nil; j++ {
			val.keys[j], err = recPtr.Key(j)
			if err == nil && j > 0 {
				val.keys[j] = append(val.keys[j], primaryKey...)
			}
		}
	}
	return
}

// concat returns the concatenaton of all specified byte slices
func concat(sls ...[]byte) (res []byte) {
	for _, sl := range sls {
		res = append(res, sl...)
	}
	return
}

// valGet generates a record's storable data and keys from an application
// record.
func valGet(recPtr Record, count uint8) (val valType, err error) {
	var j uint8
	val.data, err = recPtr.MarshalBinary()
	if err == nil {
		val.keys = make([][]byte, count)
		for j = 0; j < count && err == nil; j++ {
			val.keys[j], err = recPtr.Key(j)
			if err == nil && j > 0 {
				val.keys[j] = concat(val.keys[j], val.keys[0])
			}
		}
	}
	return
}

// Get returns zero or more records. It calls f iteratively until f() returns
// false or no more records are found. For each call of f, the record variable
// pointed to be recPtr will be populated with a successive value from the
// database. The record order is determined by the index specified by idx. The
// first record returned is the first one that matches the initial value of the
// record pointed to by recPtr. Only the field or fields that make up the key
// associated with index idx need to be assigned initially.
func (db *DB) Get(recPtr Record, idx uint8, f func() bool) (getErr error) {
	if db.boltDB == nil {
		return ErrNotOpen
	}
	count := recPtr.IndexCount()
	if idx < count {
		getErr = db.boltDB.View(func(tx *bolt.Tx) (err error) {
			var bck bucketGrpType
			bck, err = bucketGet(recPtr, count, false, tx)
			if err == nil {
				var crs *bolt.Cursor
				var key, val []byte
				loop := true
				key, err = recPtr.Key(idx)
				if err == nil {
					crs = bck.idxs[idx].Cursor()
					key, val = crs.Seek(key)
					for key != nil && err == nil && loop {
						if idx > 0 {
							// We're using a non-primary index. The value is the primary key, so we
							// need to do another lookup to get the actual record.
							val = bck.idxs[0].Get(val)
							if val == nil {
								err = ErrMissingRecord
							}
						}
						if err == nil {
							err = recPtr.UnmarshalBinary(val)
							if err == nil {
								loop = f()
								if loop {
									key, val = crs.Next()
								}
							}
						}
					}
				}
			}
			return
		})
	} else {
		getErr = fmt.Errorf("index %d too large, must be less than %d", idx, count)
	}
	return
}

// GetRec returns zero or one record from the database. The first record that
// matches the key field or fields associated with index idx will be put in the
// variable pointed to be recPtr. In this case, an error value of nil is
// returned. If no match is found, ErrRecNotFound is returned.
func (db *DB) GetRec(recPtr Record, idx uint8) (err error) {
	var found bool
	err = db.Get(recPtr, idx, func() bool {
		found = true
		return false
	})
	if err == nil && !found {
		err = ErrRecNotFound
	}
	return
}

// Delete removes records and their associated keys from the database. recPtr
// is a pointer to a variable that will, each time f() returns true, be
// populated with a successive value to be delete. The iteration is stopped
// when f() returns false. Only the field or fields needed to generate the
// primary key (index 0) need be assigned.
func (db *DB) Delete(recPtr Record, f func() bool) (delErr error) {
	loop := true
	count := recPtr.IndexCount()
	for loop && delErr == nil {
		delErr = db.boltDB.Update(func(tx *bolt.Tx) (err error) {
			var k uint8
			var currentVal valType
			var bck bucketGrpType
			var primaryKey []byte
			var scratch Record
			bck, err = bucketGet(recPtr, count, false, tx)
			if err == nil {
				scratch = recPtr.New()
				for j := 0; j < cnLoopCount && loop && err == nil; j++ {
					loop = f()
					if loop {
						// f() returned true; this indicates that the app has populated the
						// variable pointed to by recPtr with a record to be deleted.
						primaryKey, err = recPtr.Key(0)
						if err == nil {
							currentVal, err = bck.currentGet(scratch, count, primaryKey)
							if err == nil {
								for k = 0; k < count && err == nil; k++ {
									err = bck.idxs[k].Delete(currentVal.keys[k])
									// log.Printf("Deleted %v", currentVal.keys[k])
								}
							}
						}
					}
				}
			}
			return
		})
	}
	return
}

// DeleteRec deletes one record from the database. recPtr is a pointer to a
// variable that has at least the field or fields that make up the primary key
// (index 0) assigned.
func (db *DB) DeleteRec(recPtr Record) (err error) {
	return db.Delete(recPtr, limit(1))
}

type idxPutType struct {
	bck             bucketGrpType
	recPtr, scratch Record
	f               func() bool
	count           uint8
}

func (p *idxPutType) idxPut() (err error) {
	var (
		k                  uint8
		different          bool
		addList            [256]bool
		currentVal, recVal valType
		primaryKey         []byte
	)
	recVal, err = valGet(p.recPtr, p.count)
	if err == nil {
		if err == nil {
			primaryKey = recVal.keys[0]
			currentVal, err = p.bck.currentGet(p.scratch, p.count, primaryKey)
			if err == nil {
				// For now, assume that record's data and keys have at least some
				// differences with those of the currently stored version
				addList[0] = true
				if currentVal.data == nil {
					// Record is new: mark all keys for insertion
					// log.Printf("New [%s]", p.recPtr)
					for k = 1; k < p.count; k++ {
						addList[k] = true
					}
				} else {
					// testReconstruct(p.scratch, currentVal.data)
					// Record is present in database: remove obsolete keys and mark them
					// for replacement
					for k = 1; k < p.count && err == nil; k++ {
						// log.Printf("Comparing %v : %v", currentVal.keys[k], recVal.keys[k])
						different = !bytes.Equal(currentVal.keys[k], recVal.keys[k])
						addList[k] = different
						if different {
							err = p.bck.idxs[k].Delete(currentVal.keys[k])
							// log.Printf("Changed %v : %v", currentVal.keys[k], recVal.keys[k])
						}
					}

				}
				// Assume no value means record is new; put keys as usual
				// Otherwise:
				// Compare keys, for each index in which the stored key is different than the
				// buffer key, delete the stored key and put the buffered key. Equal keys can be
				// ignored.
				if err == nil {
					err = p.bck.idxs[0].Put(recVal.keys[0], recVal.data)
					for k = 1; k < p.count && err == nil; k++ {
						if addList[k] {
							err = p.bck.idxs[k].Put(recVal.keys[k], primaryKey)
						}
					}
				}
			}
		}
	}
	return
}

// recPut is the backing method for Add and Put.
func (db *DB) recPut(recPtr Record, f func() bool, add bool) (putErr error) {
	if db.boltDB == nil {
		return ErrNotOpen
	}
	var put idxPutType
	put.recPtr = recPtr
	put.f = f
	loop := true
	createIfNeeded := true
	put.count = recPtr.IndexCount()
	for loop && putErr == nil {
		putErr = db.boltDB.Update(func(tx *bolt.Tx) (err error) {
			put.bck, err = bucketGet(recPtr, put.count, createIfNeeded, tx)
			if err == nil {
				put.scratch = recPtr.New()
				createIfNeeded = false
				for j := 0; j < cnLoopCount && loop && err == nil; j++ {
					loop = f()
					if loop {
						// f() returned true; this indicates that the app has populated the
						// variable pointed to by recPtr with a record to be stored. If the
						// record is being inserted through a call to Add(), pass an
						// autoincremented ID to the application now.
						if add {
							var autoID uint64
							autoID, err = put.bck.idxs[0].NextSequence()
							if err == nil {
								recPtr.NextID(autoID)
							}
						}
						if err == nil {
							err = put.idxPut()
						}
					}
				} // loop
			}
			return
		})
	}
	return
}

// Put inserts or replaces zero or more records in the database. recPtr is a
// pointer to a variable that will, each time f() returns true, be populated
// with a successive value to be stored. The iteration is stopped when f()
// returns false. If the primary key (that is, Key(0)) of a record already
// exists, the record will overwrite its previous value. If the primary key is
// unique, the record will be inserted; however, unlike Add(), the inserted
// record's NextID() method will not be called. It is crucial that all keys of
// each record processed by this method be properly assigned. This assures that
// modified keys are properly replaced.
func (db *DB) Put(recPtr Record, f func() bool) (putErr error) {
	return db.recPut(recPtr, f, false)
}

// PutRec inserts or replaces one record in the database. recPtr is a pointer
// to a variable that fully assigned. The requirements documented for Put()
// apply.
func (db *DB) PutRec(recPtr Record) (err error) {
	return db.Put(recPtr, limit(1))
}

// Add inserts one or more records in the database. It functions like Put()
// except that Add() will pass an autoincremented ID by means of the NextID
// interface method. If the application manages its own unique primary keys, it
// is more efficient to call Put() instead of Add(). It is crucial that all
// keys of each record processed by this method be properly assigned. This
// assures that modified keys are properly replaced.
func (db *DB) Add(recPtr Record, f func() bool) (putErr error) {
	return db.recPut(recPtr, f, true)
}

// AddRec inserts one record in the database. recPtr is a pointer to a variable
// that fully assigned. The requirements documented for Add() apply.
func (db *DB) AddRec(recPtr Record) (err error) {
	return db.Add(recPtr, limit(1))
}

// limit returns a function that returns true the first count times it is
// called and false thereafter.
func limit(count int) func() bool {
	return func() bool {
		if count > 0 {
			count--
			return true
		}
		return false
	}
}

// hexView is the worker function for HexDump.
func hexView(wr io.Writer, crs *bolt.Cursor, indent int) {
	var k, v []byte
	k, v = crs.First()
	for k != nil {
		hexdump(wr, k, "Key", indent, 2)
		if v == nil {
			bck := crs.Bucket().Bucket(k)
			if bck != nil {
				hexView(wr, bck.Cursor(), indent+1)
			}
		} else {
			hexdump(wr, v, "Data", indent, 2)
		}
		k, v = crs.Next()
	}
}

// HexDump is a diagnostic routine to help with viewing the records and keys in
// a database.
func (db *DB) HexDump(wr io.Writer) {
	if db.boltDB != nil {
		db.boltDB.View(func(tx *bolt.Tx) (err error) {
			hexView(wr, tx.Cursor(), 0)
			return
		})
	}
}

// Close shuts down the database and releases all associated resources. Any
// subsequent calls to methods of DB will result in an error.
func (db *DB) Close() (err error) {
	if db.boltDB != nil {
		err = db.boltDB.Close()
		db.boltDB = nil
	} else {
		err = ErrNotOpen
	}
	return
}

func exists(path string) (ok bool) {
	var err error
	var info os.FileInfo
	info, err = os.Stat(path)
	if err == nil {
		// if os.IsExist(err) {
		ok = info.Mode().IsRegular()
	}
	return
}

func open(path string, mode os.FileMode, options Options) (db *DB, err error) {
	db = new(DB)
	db.boltDB, err = bolt.Open(path, mode, &options.BoltOpt)
	if err == nil {
		db.opt = options
	} else {
		db = nil
	}
	return
}

// Open opens an existing Pinion database.
func Open(path string, mode os.FileMode, options Options) (db *DB, err error) {
	if exists(path) {
		db, err = open(path, mode, options)
	} else {
		err = fmt.Errorf("file \"%s\" does not exist", path)
	}
	return
}

// Create creates a Pinion database. The file is replaced if it already exists.
func Create(path string, mode os.FileMode, options Options) (db *DB, err error) {
	if exists(path) {
		err = os.Remove(path)
	}
	if err == nil {
		db, err = open(path, mode, options)
	}
	return
}
