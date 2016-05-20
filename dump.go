package pinion

import (
	"fmt"
	"io"
	"strings"
)

func hexdump(wr io.Writer, sl []byte, hdrStr string, indent, maxIndent int) {
	var count, j, pos, rowCount, row int
	var val byte
	var indentStr, padStr string
	var ascii [16]byte
	count = len(sl)
	rowCount = (count + 15) >> 4
	// 00000000  42 5a 68 39 31 41 59 26  53 59 c9 f4 b8 1a 02 62  |BZh91AY&SY.....b|
	// 00000010  32 5f 80 00 10 41 84 7f  e0 3f ff ff f0 3f ff ff  |2_...A...?...?..|
	if indent > 0 {
		indentStr = strings.Repeat("  ", indent)
	}
	if indent < maxIndent {
		padStr = strings.Repeat("  ", maxIndent-indent)
	}
	padStr += " |"
	if hdrStr != "" {
		fmt.Fprintf(wr, "%s%s\n", indentStr, hdrStr)
	}
	for row = 0; row < rowCount; row++ {
		fmt.Fprintf(wr, "%s%08x  ", indentStr, pos)
		for j = 0; j < 16; j++ {
			if pos < count {
				val = sl[pos]
				fmt.Fprintf(wr, "%02x ", val)
				if val > 31 && val < 127 {
					ascii[j] = val
				} else {
					ascii[j] = 46
				}
				pos++
			} else {
				io.WriteString(wr, "   ")
				ascii[j] = 32
			}
			if j == 7 {
				io.WriteString(wr, " ")
			}
		}
		io.WriteString(wr, padStr)
		wr.Write(ascii[:])
		io.WriteString(wr, "|\n")
	}
}
