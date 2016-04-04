package str_test

import (
	"fmt"
	"testing"

	"github.com/piniondb/pinion/internal/str"
)

func intStr(val uint) string {
	return str.Delimit(fmt.Sprintf("%d", val), ",", 3)
}

func Example_quantity() {
	for _, val := range []uint{0, 5, 15, 121, 4320, 70123,
		999321, 4032500, 50100438, 100000054} {
		fmt.Printf("[%14s] [%s]\n", intStr(val), str.Quantity(val))
	}
	// Output:
	// [             0] [zero]
	// [             5] [five]
	// [            15] [fifteen]
	// [           121] [one hundred twenty one]
	// [         4,320] [four thousand three hundred twenty]
	// [        70,123] [seventy thousand one hundred twenty three]
	// [       999,321] [nine hundred ninety nine thousand three hundred twenty one]
	// [     4,032,500] [four million thirty two thousand five hundred]
	// [    50,100,438] [fifty million one hundred thousand four hundred thirty eight]
	// [   100,000,054] [one hundred million fifty four]
}

func Example_quantityEncode() {
	var sl []byte
	var err error
	for _, val := range []uint{0, 5, 15, 121, 4320, 70123,
		999321, 4032500, 50100438, 100000054} {
		sl, err = str.QuantityEncode(val)
		if err == nil {
			fmt.Printf("[%14s] [%s]\n", intStr(val), str.QuantityDecode(sl))
		}
	}
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// [             0] [zero]
	// [             5] [five]
	// [            15] [fifteen]
	// [           121] [one hundred twenty one]
	// [         4,320] [four thousand three hundred twenty]
	// [        70,123] [seventy thousand one hundred twenty three]
	// [       999,321] [nine hundred ninety nine thousand three hundred twenty one]
	// [     4,032,500] [four million thirty two thousand five hundred]
	// [    50,100,438] [fifty million one hundred thousand four hundred thirty eight]
	// [   100,000,054] [one hundred million fifty four]
}

func quantityExpect(t *testing.T, val uint, expStr string) {
	valStr := str.Quantity(val)
	if valStr != expStr {
		t.Fatalf("expecting \"%s\", got \"%s\"", expStr, valStr)
	}
}

// Test return value of Quantity
func TestQuantity(t *testing.T) {
	quantityExpect(t, 35, "thirty five")
	quantityExpect(t, 100000000, "one hundred million")
	quantityExpect(t, 2100200300, "2100200300")
}

// Test return value of Quantity
func TestQuantityEncode(t *testing.T) {
	var err error
	_, err = str.QuantityEncode(35)
	if err != nil {
		t.Fatal(err)
	}
	_, err = str.QuantityEncode(2100200300)
	if err == nil {
		t.Fatalf("expecting over-limit error")
	}
}
