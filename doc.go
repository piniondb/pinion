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

/*
Package pinion provides a fast and simple set of routines to manage the storage
and retrieval of structured records.

Overview

Pinion automates the task of managing record storage and multiple retrieval
indexes. Its simple programming interface, comprising methods like Put() and
Get(), operate on types that implement the pinion.Record interface. This
interface isolates key-building and value-encoding to one place in your
application. When used with the piniondb/store package, a fast implementation
of the interface can be made simply that does not require reflection. Any
practical number of record types that satisfy the pinion.Record interface can
be managed by a pinion database.

Currently, pinion does not support joined records. This is obviated to some
degree with its support for structures that may include maps and slices.

The pinion package depends on boltdb. All tests pass on Linux, Mac and Windows
platforms.

Example

This example manages records of type personType. This type implements the
pinion.Record interface; see the heavily commented file person_test.go for more
details. Note that no type assertions or explicit encoding or decoding needs to
be done to populate and retrieve records.

	var db *pinion.DB
	var err error
	var person personType
	db, err = pinion.Create("example/standalobe.db", 0600, pinion.Options{})
	if err == nil {
		wdb := db.Wrap()
		list := []nameType{
			{last: "Smith", middle: "J", first: "Carol"},
			{last: "Jones", middle: "W", first: "Robert"},
		}
		wdb.Add(&person, func() bool {
			if len(list) > 0 {
				person = personType{id: 0, name: list[0]}
				list = list[1:]
				return true
			}
			return false
		})
		for idx := uint8(0); idx < idxPersonCount; idx++ {
			fmt.Printf("%-12s", personIndexNames[idx])
			person = personType{} // Start search at beginning with zeroed record
			wdb.Get(&person, idx, func() bool {
				fmt.Printf(" [%s]", person)
				return true
			})
			fmt.Println("")
		}
	}
	if err != nil {
		fmt.Println(err)
	}

Running this example produces the following output:

	Output:
	ID           [Carol J Smith / 1] [Robert W Jones / 2]
	Last name    [Robert W Jones / 2] [Carol J Smith / 1]
	First name   [Carol J Smith / 1] [Robert W Jones / 2]

Installation

To install the package on your system, run

	go get github.com/piniondb/pinion

Errors

The methods of a *pinion.DB instance return an error if the operation fails.
Since database activity often involves a lot of steps, you may find it useful
to locally wrap the database instance with Wrap() in order to defer error
handling to a single place.

Keys

In addition to the required primary index, up to 255 secondary indexes can be
defined for the record type you want to manage. Only keys in the primary index
(index 0) need to be unique. Keys must be sortable when inserted into the
underlying database as byte slices. The piniondb/store package provides support
for fixed-length key segments. Alternatively, you can use fmt.Sprintf() to
format fixed-length fields.

Best practices

• Implement the pinion.Record interface in the same location at which the
structure itself is defined

• When working with multiple records, single calls to Add(), Put() and Get()
will be faster than individual calls to AddRec(), PutRec() and GetRec().

Contributing Changes

pinion is a global community effort and you are invited to make it even better.
If you have implemented a new feature or corrected a problem, please consider
contributing your change to the project. Your pull request should

• be compatible with the MIT License

• be properly documented

• include an example in one of the test files (for example, pinion_test.go)
if appropriate

Use https://goreportcard.com/report/github.com/piniondb/pinion to assure that no
compliance issues have been introduced.

License

pinion is released under the MIT License.

*/
package pinion
