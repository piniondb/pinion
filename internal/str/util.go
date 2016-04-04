package str

import (
	"bytes"
	"fmt"
	"strings"
)

// Delimit converts 'ABCDEFG' to, for example, 'A,BCD,EFG'
func Delimit(str string, sepstr string, sepcount int) string {
	pos := len(str) - sepcount
	for pos > 0 {
		str = str[:pos] + sepstr + str[pos:]
		pos = pos - sepcount
	}
	return str
}

var lookup = [...]string{
	"",           // 0
	"eight ",     // 1
	"eighteen ",  // 2
	"eighty ",    // 3
	"eleven ",    // 4
	"fifteen ",   // 5
	"fifty ",     // 6
	"five ",      // 7
	"forty ",     // 8
	"four ",      // 9
	"fourteen ",  // 10
	"hundred ",   // 11
	"million ",   // 12
	"nine ",      // 13
	"nineteen ",  // 14
	"ninety ",    // 15
	"one ",       // 16
	"seven ",     // 17
	"seventeen ", // 18
	"seventy ",   // 19
	"six ",       // 20
	"sixteen ",   // 21
	"sixty ",     // 22
	"ten ",       // 23
	"ten ",       // 24
	"thirteen ",  // 25
	"thirty ",    // 26
	"thousand ",  // 27
	"three ",     // 28
	"twelve ",    // 29
	"twenty ",    // 30
	"two ",       // 31
	"zero ",      // 32
}

// QuantityDecode returns the textual expansion of the byte sequence specified
// by sl. See QuantityEncode for more details.
func QuantityDecode(sl []byte) string {
	var buf bytes.Buffer
	for _, c := range sl {
		if c < 33 {
			buf.WriteString(lookup[c])
		}
	}
	return strings.TrimSpace(buf.String())
}

func quantityByte(buf *bytes.Buffer, val uint) {
	var numList = [...]byte{0, 16, 31, 28, 9, 7, 20, 17, 1, 13, 24, 4, 29,
		25, 10, 5, 21, 18, 2, 14}
	var tenList = [...]byte{23, 30, 26, 8, 6, 22, 19, 3, 15}

	if val >= 1000000 {
		quantityByte(buf, val/1000000)
		buf.WriteByte(12)
		quantityByte(buf, val%1000000)
	} else if val >= 1000 {
		quantityByte(buf, val/1000)
		buf.WriteByte(27)
		quantityByte(buf, val%1000)
	} else if val >= 100 {
		quantityByte(buf, val/100)
		buf.WriteByte(11)
		quantityByte(buf, val%100)
	} else if val >= 20 {
		buf.WriteByte(tenList[val/10-1])
		quantityByte(buf, val%10)
	} else {
		buf.WriteByte(numList[val])
	}
}

// QuantityEncode returns the English text equivalent, in byte-encoded form, of
// the number specified by val. For example, if val is 21, the return value is
// "twenty one". Sorting encoded values for various values produces the same
// order as sorting decoded values. QuantityDecode() decodes the packed byte
// sequence into English words. val must be a value less than or equal to
// 999,999,999.
func QuantityEncode(val uint) (sl []byte, err error) {
	const limit = 1000000000
	if val < limit {
		var buf bytes.Buffer
		if val > 0 {
			quantityByte(&buf, val)
		} else {
			buf.WriteByte(32)
		}
		sl = buf.Bytes()
	} else {
		err = fmt.Errorf("expecting val (%d) to be less than %d", val, limit)
	}
	return
}
func quantity(buf *bytes.Buffer, val uint) {
	var numList = [...]string{"", "one ", "two ", "three ", "four ",
		"five ", "six ", "seven ", "eight ", "nine ", "ten ", "eleven ", "twelve ",
		"thirteen ", "fourteen ", "fifteen ", "sixteen ", "seventeen ", "eighteen ",
		"nineteen "}
	var tenList = [...]string{"ten ", "twenty ", "thirty ", "forty ",
		"fifty ", "sixty ", "seventy ", "eighty ", "ninety "}

	if val >= 1000000 {
		quantity(buf, val/1000000)
		fmt.Fprint(buf, "million ")
		quantity(buf, val%1000000)
	} else if val >= 1000 {
		quantity(buf, val/1000)
		fmt.Fprint(buf, "thousand ")
		quantity(buf, val%1000)
	} else if val >= 100 {
		quantity(buf, val/100)
		fmt.Fprint(buf, "hundred ")
		quantity(buf, val%100)
	} else if val >= 20 {
		fmt.Fprint(buf, tenList[val/10-1])
		quantity(buf, val%10)
	} else {
		fmt.Fprint(buf, numList[val])
	}
}

// Quantity returns the English text equivalent of the number specified by
// val. For example, if val is 35 the return value is "thirty five".
func Quantity(val uint) string {
	if val == 0 {
		return "zero"
	} else if val >= 1000000000 {
		return fmt.Sprintf("%d", val)
	} else {
		var buf bytes.Buffer
		quantity(&buf, val)
		return strings.TrimSpace(buf.String())
	}
}
