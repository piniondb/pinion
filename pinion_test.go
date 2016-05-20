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

package pinion_test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/piniondb/pinion"
	"github.com/piniondb/str"
)

func quantityRec(id uint32) (q quantityType) {
	q.id = id
	q.val, _ = str.QuantityEncode(uint(id))
	return
}

type ticker time.Time

func (t ticker) elapsed() string {
	return time.Since(time.Time(t)).String()
}

func (t ticker) elapsedAverage(count int) string {
	d := time.Since(time.Time(t))
	if count > 0 {
		opD := d / time.Duration(count)
		return fmt.Sprintf("%s (%s/op)", d, opD)
	}
	return d.String()
}

func timer() ticker {
	return ticker(time.Now())
}

// Create a database and populate it with quantity records ranging from lo to
// hi inclusive. If no error occurs, the open database instance is returned
// followed by nil. Otherwise, nil is returned followed by an error value.
func quantityDB(fileStr string, lo, hi uint32) (db *pinion.DB, err error) {
	db, err = pinion.Create(fileStr, 0600, pinion.Options{})
	if err == nil {
		var q quantityType
		err = db.Put(&q, func() bool {
			if lo <= hi {
				q = quantityRec(lo)
				lo++
				return true
			}
			return false
		})
		if err != nil {
			db.Close()
		}
	}
	return
}

// ExampleDB_quantity demonstrates various operations with multiple indexes.
func ExampleDB_quantity() {
	var db *pinion.DB
	var wdb *pinion.WrapDB
	var err error
	db, err = quantityDB("example/quantity.db", 0, 256)
	if err == nil {
		wdb = db.Wrap()
		var q quantityType
		q.id = 99
		fmt.Println("--- ID sequence ---")
		wdb.Get(&q, idxQuantityID, func() bool {
			if q.id < 104 {
				fmt.Println(q)
				return true
			}
			return false
		})
		q.val, _ = str.QuantityEncode(72)
		count := 5
		fmt.Println("--- Word sequence ---")
		wdb.Get(&q, idxQuantityVal, func() bool {
			if count > 0 {
				fmt.Println(q)
				count--
				return true
			}
			return false
		})
		db.Close()
		err = wdb.Error()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// --- ID sequence ---
	// [         99 : ninety nine]
	// [        100 : one hundred]
	// [        101 : one hundred one]
	// [        102 : one hundred two]
	// [        103 : one hundred three]
	// --- Word sequence ---
	// [         72 : seventy two]
	// [          6 : six]
	// [         16 : sixteen]
	// [         60 : sixty]
	// [         68 : sixty eight]
}

// This code exemplifies the use of various WrapDB methods. These are like
// corresponding DB methods except that error values are not returned. Instead,
// they retain the error value internally. In this example, the DB instance is
// local. In a typical application, the database instance is global so that it
// can be shared among goroutines. Unlike the DB instance, a WrapDB instance
// should always be local so that errors can be managed locally and do not
// spill over to other goroutines.
func ExampleWrapDB() {
	var (
		q    quantityType
		db   *pinion.DB
		wdb  *pinion.WrapDB
		fl   *os.File
		err  error
		list []uint32
	)
	show := func(str string) {
		fmt.Printf("--- %s ---\n", str)
		q = quantityType{} // Start at beginning with zeroed record
		wdb.Get(&q, idxQuantityID, func() bool {
			fmt.Println(q)
			return true
		})
	}
	db, err = quantityDB("example/dump.db", 1234, 1236)
	if err == nil {
		wdb = db.Wrap()
		list = []uint32{42, 0}
		wdb.Put(&q, func() bool {
			if len(list) > 0 {
				q = quantityRec(list[0])
				list = list[1:]
				return true
			}
			return false
		})
		fl, err = os.Create("example/dump.txt")
		if err == nil {
			wdb.HexDump(fl)
			show("Full")
			list = []uint32{1235, 0}
			wdb.Delete(&q, func() bool {
				if len(list) > 0 {
					q.id = list[0]
					list = list[1:]
					return true
				}
				return false
			})
			show("Deleted ID 0 and 1235")
			q = quantityRec(1232)
			wdb.AddRec(&q)
			show("Added 1232")
			q.id = 42
			wdb.DeleteRec(&q)
			show("Deleted ID 42")
			fl.Close()
			if wdb.Error() == nil {
				wdb.ErrorSet(pinion.ErrRecNotFound)
				if wdb.Error() != nil {
					wdb.ErrorClear()
				}
			}
			err = wdb.Error()
		}
		db.Close()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// --- Full ---
	// [          0 : zero]
	// [         42 : forty two]
	// [      1,234 : one thousand two hundred thirty four]
	// [      1,235 : one thousand two hundred thirty five]
	// [      1,236 : one thousand two hundred thirty six]
	// --- Deleted ID 0 and 1235 ---
	// [         42 : forty two]
	// [      1,234 : one thousand two hundred thirty four]
	// [      1,236 : one thousand two hundred thirty six]
	// --- Added 1232 ---
	// [         42 : forty two]
	// [      1,232 : one thousand two hundred thirty two]
	// [      1,234 : one thousand two hundred thirty four]
	// [      1,236 : one thousand two hundred thirty six]
	// --- Deleted ID 42 ---
	// [      1,232 : one thousand two hundred thirty two]
	// [      1,234 : one thousand two hundred thirty four]
	// [      1,236 : one thousand two hundred thirty six]
}

// BenchmarkStoreRoundtrip times the foobar operation.
func BenchmarkStoreRoundtrip(b *testing.B) {
	var db *pinion.DB
	var err error
	const fileStr = "example/test.db"
	db, err = pinion.Create(fileStr, 0600, pinion.Options{})
	if err == nil {
		db.Close()
		for j := 0; j < b.N && err == nil; j++ {
			db, err = pinion.Open("example/test.db", 0600, pinion.Options{})
			if err == nil {
				db.Close()
			}
		}
	}
	if err != nil {
		b.Error(err)
	}
}

func notDatabase(t *testing.T) {
	var err error
	var fileStr = "README.md"
	_, err = pinion.Open(fileStr, 0600, pinion.Options{})
	if err == nil {
		t.Fatalf("should not have been able to open %s as database", fileStr)
	}
}

func nonexistentDatabase(t *testing.T) {
	var db *pinion.DB
	var err error
	var fileStr = "example/nonexistent/errors.db"
	db, err = pinion.Open(fileStr, 0600, pinion.Options{})
	if err == nil {
		db.Close()
		t.Fatalf("should not have been able to open %s", fileStr)
	}
}

func accessErrors(t *testing.T) {
	var db *pinion.DB
	var err error
	var fileStr = "example/errors.db"
	db, err = pinion.Create(fileStr, 0600, pinion.Options{})
	if err == nil {
		q := quantityRec(1)
		err = db.PutRec(&q)
		if err == nil {
			q.id = 2
			err = db.GetRec(&q, idxQuantityID)
			if err == nil {
				t.Fatal("should not have been able to retrieve beyond last record")
			}
			q.id = 1
			err = db.GetRec(&q, 42)
			if err == nil {
				t.Fatal("should not have been able to use out-of-bounds index")
			}
			err = db.Close()
			if err == nil {
				q.id = 1
				err = db.GetRec(&q, idxQuantityID)
				if err == nil {
					t.Fatal("should not have been able to retrieve record from closed database")
				}
				q := quantityRec(2)
				err = db.PutRec(&q)
				if err == nil {
					t.Fatalf("should not have been able to put record into closed database")
				}
				err = db.Close()
				if err == nil {
					t.Fatalf("should not have been able to over-close %s", fileStr)
				} else {
					err = nil
				}
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}

type badQuantityType struct {
	quantityType
}

func (b badQuantityType) IndexCount() uint8 {
	return 0
}

// Intentionally exercise an internal index count error
func indexError(t *testing.T) {
	var db *pinion.DB
	var err error
	var fileStr = "example/indexcount.db"
	db, err = pinion.Create(fileStr, 0600, pinion.Options{})
	if err == nil {
		var q badQuantityType
		q.quantityType = quantityRec(123)
		err = db.PutRec(&q)
		if err == nil {
			t.Fatalf("zero index record should not be processable")
		} else {
			err = nil
		}
		db.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
}

func rawManipulate(fileStr string, j int) (err error) {
	var bdb *bolt.DB
	var q quantityType
	bdb, err = bolt.Open(fileStr, 0600, nil)
	if err == nil {
		err = bdb.Update(func(tx *bolt.Tx) error {
			var err error
			switch j {
			case 0:
				// Delete record bucket
				err = tx.DeleteBucket([]byte(q.Name()))
			case 1:
				// Delete record data bucket
				var bck *bolt.Bucket
				bck = tx.Bucket([]byte(q.Name()))
				if bck != nil {
					err = bck.DeleteBucket([]byte{0})
				} else {
					err = pinion.ErrRecNotFound
				}
			case 2:
				// Corrupt non-primary reference
				var bck *bolt.Bucket
				// var crs *bolt.Cursor
				var k []byte
				err = pinion.ErrRecNotFound
				bck = tx.Bucket([]byte(q.Name()))
				if bck != nil {
					bck = bck.Bucket([]byte{1})
					if bck != nil {
						k, _ = bck.Cursor().First()
						if k != nil {
							err = bck.Put(k, []byte{255})
						}
					}
				}
			}
			return err
		})
		bdb.Close()
	}
	return err
}

// Intentionally corrupt pinion's database model by modifying database with
// bolt API
func internalErrors(t *testing.T) {
	var db *pinion.DB
	var j int
	var err error
	var q quantityType
	var fileStr = "example/internal.db"
	for j = 0; j < 3 && err == nil; j++ {
		db, err = quantityDB(fileStr, 1000, 1005)
		if err == nil {
			db.Close()
			err = rawManipulate(fileStr, j)
			if err == nil {
				db, err = pinion.Open(fileStr, 0600, pinion.Options{})
				if err == nil {
					switch j {
					case 2:
						q = quantityType{}
						err = db.GetRec(&q, idxQuantityVal)
					default:
						q.id = 1000
						err = db.GetRec(&q, idxQuantityID)
					}
					if err == nil {
						t.Fatalf("should not have been able to retrieve record with missing bucket or record")
					} else {
						err = nil
					}
					db.Close()
				}
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}

func seq(count int) (sl []int) {
	sl = make([]int, count)
	for j := 0; j < count; j++ {
		sl[j] = j
	}
	return
}

// Test large number of records
func TestDB_ManyRecs(t *testing.T) {
	var count = 256
	var double = 10
	const fileStr = "example/manyrec.db"
	var db *pinion.DB
	var wdb *pinion.WrapDB
	var err error
	var q quantityType
	var tck ticker
	for double > 0 && err == nil {
		prm := rand.Perm(count)
		// prm := seq(count)
		db, err = pinion.Create(fileStr, 0600, pinion.Options{})
		if err == nil {
			wdb = db.Wrap()
			fmt.Printf("Record count: %s\n", intStr(uint32(count)))
			fmt.Printf("  Adding...")
			tck = timer()
			wdb.Put(&q, func() bool {
				if len(prm) > 0 {
					q = quantityRec(uint32(prm[0]))
					prm = prm[1:]
					return true
				}
				return false
			})
			fmt.Printf("%s\n", tck.elapsedAverage(count))
			var prevID uint32
			q = quantityType{}
			fmt.Printf("  Verifying ID index...")
			tck = timer()
			wdb.Get(&q, idxQuantityID, func() bool {
				if q.id > 0 {
					if q.id <= prevID {
						wdb.ErrorSet(fmt.Errorf("out-of-order ID index at ID = %d", q.id))
					}
					prevID = q.id
				}
				return true
			})
			fmt.Printf("%s\n", tck.elapsedAverage(count))
			var valStr, prevStr string
			q = quantityType{}
			fmt.Printf("  Verifying English word index...")
			tck = timer()
			wdb.Get(&q, idxQuantityVal, func() bool {
				valStr = str.QuantityDecode(q.val)
				if valStr <= prevStr {
					wdb.ErrorSet(fmt.Errorf("out-of-order value index at %s", valStr))
				}
				prevStr = valStr
				return true
			})
			fmt.Printf("%s\n", tck.elapsedAverage(count))
			db.Close()
			err = wdb.Error()
		}
		count *= 2
		double--
	}
	if err != nil {
		t.Fatal(err)
	}
}

// Test large number of small records with no secondary indexes
func TestDB_ManySmallRecs(t *testing.T) {
	var count = 256
	var double = 13
	const fileStr = "example/manysmallrec.db"
	var db *pinion.DB
	var wdb *pinion.WrapDB
	var err error
	var i intType
	var tck ticker
	for double > 0 && err == nil {
		// prm := rand.Perm(count)
		prm := seq(count)
		db, err = pinion.Create(fileStr, 0600, pinion.Options{})
		if err == nil {
			wdb = db.Wrap()
			fmt.Printf("Record count: %s\n", intStr(uint32(count)))
			fmt.Printf("  Adding...")
			tck = timer()
			wdb.Put(&i, func() bool {
				if len(prm) > 0 {
					i.id = uint32(prm[0])
					prm = prm[1:]
					return true
				}
				return false
			})
			fmt.Printf("%s\n", tck.elapsedAverage(count))
			var prevID uint32
			i = intType{}
			fmt.Printf("  Verifying ID index...")
			tck = timer()
			wdb.Get(&i, idxIntID, func() bool {
				if i.id > 0 {
					if i.id <= prevID {
						wdb.ErrorSet(fmt.Errorf("out-of-order ID index at ID = %d", i.id))
					}
					prevID = i.id
				}
				return true
			})
			fmt.Printf("%s\n", tck.elapsedAverage(count))
			db.Close()
			err = wdb.Error()
		}
		count *= 2
		double--
	}
	if err != nil {
		t.Fatal(err)
	}
}

// Test various errors
func TestDB_Errors(t *testing.T) {
	notDatabase(t)
	nonexistentDatabase(t)
	accessErrors(t)
	internalErrors(t)
	indexError(t)
}
