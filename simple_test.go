package pinion_test

import (
	"fmt"
	"github.com/piniondb/pinion"
)

func populate(wdb *pinion.WrapDB) {
	var person personType
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
	show(wdb, "Initial")
}

func change(wdb *pinion.WrapDB) {
	var person personType
	person.name.first = "Robert"
	wdb.GetRec(&person, idxPersonNameFirst) // Get only first match
	if person.id > 0 {                      // Found the record
		person.name.first = "Bob"
		wdb.PutRec(&person)
		show(wdb, "Robert changed to Bob")
	}
}

func show(wdb *pinion.WrapDB, hdrStr string) {
	var person personType
	fmt.Printf("--- %s ---\n", hdrStr)
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

// Example demonstrates record management with pinion.
func Example() {
	var db *pinion.DB
	var err error
	db, err = pinion.Create("example/person.db", 0600, pinion.Options{})
	if err == nil {
		wdb := db.Wrap()
		populate(wdb)
		change(wdb)
		db.Close()
		err = wdb.Error()
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// --- Initial ---
	// ID           [Carol J Smith / 1] [Robert W Jones / 2]
	// Last name    [Robert W Jones / 2] [Carol J Smith / 1]
	// First name   [Carol J Smith / 1] [Robert W Jones / 2]
	// --- Robert changed to Bob ---
	// ID           [Carol J Smith / 1] [Bob W Jones / 2]
	// Last name    [Bob W Jones / 2] [Carol J Smith / 1]
	// First name   [Bob W Jones / 2] [Carol J Smith / 1]
}

func Example_standalone() {
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
	// Output:
	// ID           [Carol J Smith / 1] [Robert W Jones / 2]
	// Last name    [Robert W Jones / 2] [Carol J Smith / 1]
	// First name   [Carol J Smith / 1] [Robert W Jones / 2]
}
