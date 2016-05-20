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
	"io"
)

// WrapDB is a wrapper around DB that maintains error state internally. Its
// methods are the same as those of DB except that they do not return an error
// value. Instead, if an error is detected, it is stored for later examination.
// In this case, subsequent calls will be bypassed. The use of this wrapper may
// simplify code paths by deferring error handling until a series of database
// operations have completed.
//
// Unlike a DB instance, a WrapDB instance is not safe for concurrent use. It
// is intended to be used locally for a relatively small sequence of method
// calls and then, after examining the error value returned by Error(), allowed
// to fall out of scope. Multiple goroutines may wrap a single *pinion.DB
// instance concurrently.
type WrapDB struct {
	hnd *DB
	err error
}

// Wrap returns a wrapped database instance that simplifies error handling.
func (db *DB) Wrap() (wdb *WrapDB) {
	wdb = new(WrapDB)
	wdb.hnd = db
	return
}

// ErrorSet allows the application to transfer its own error to the wrapped
// database instance. This may simplify code paths in the application because
// it allows the response to an error to be handled in one place. WrapDB cannot
// already be in an error state. If err is nil, it is ignored and will not
// overwrite the internal error value.
func (wdb *WrapDB) ErrorSet(err error) {
	if wdb.err == nil && err != nil {
		wdb.err = err
	}
}

// ErrorClear clears the internal error value. The current value before being
// cleared is returned.
func (wdb *WrapDB) ErrorClear() (err error) {
	err = wdb.err
	wdb.err = nil
	return
}

// Error returns the internal error value. It does not change the internal
// value.
func (wdb *WrapDB) Error() error {
	return wdb.err
}

// Get is the locally-wrapped version of *DB.Get().
func (wdb *WrapDB) Get(recPtr Record, idx uint8, f func() bool) {
	if wdb.err == nil {
		wdb.err = wdb.hnd.Get(recPtr, idx, f)
	}
}

// GetRec is the locally-wrapped version of *DB.GetRec().
func (wdb *WrapDB) GetRec(recPtr Record, idx uint8) {
	if wdb.err == nil {
		wdb.err = wdb.hnd.GetRec(recPtr, idx)
	}
}

// Delete is the locally-wrapped version of *DB.Delete().
func (wdb *WrapDB) Delete(recPtr Record, f func() bool) {
	if wdb.err == nil {
		wdb.err = wdb.hnd.Delete(recPtr, f)
	}
}

// DeleteRec is the locally-wrapped version of *DB.DeleteRec().
func (wdb *WrapDB) DeleteRec(recPtr Record) {
	if wdb.err == nil {
		wdb.err = wdb.hnd.DeleteRec(recPtr)
	}
}

// Put is the locally-wrapped version of *DB.Put().
func (wdb *WrapDB) Put(recPtr Record, f func() bool) {
	if wdb.err == nil {
		wdb.err = wdb.hnd.Put(recPtr, f)
	}
}

// PutRec is the locally-wrapped version of *DB.PutRec().
func (wdb *WrapDB) PutRec(recPtr Record) {
	if wdb.err == nil {
		wdb.err = wdb.hnd.PutRec(recPtr)
	}
}

// Add is the locally-wrapped version of *DB.Add().
func (wdb *WrapDB) Add(recPtr Record, f func() bool) {
	if wdb.err == nil {
		wdb.err = wdb.hnd.Add(recPtr, f)
	}
}

// AddRec is the locally-wrapped version of *DB.AddRec().
func (wdb *WrapDB) AddRec(recPtr Record) {
	if wdb.err == nil {
		wdb.err = wdb.hnd.AddRec(recPtr)
	}
}

// HexDump is the locally-wrapped version of *DB.HexDump().
func (wdb *WrapDB) HexDump(wr io.Writer) {
	if wdb.err == nil {
		wdb.hnd.HexDump(wr)
	}
}
