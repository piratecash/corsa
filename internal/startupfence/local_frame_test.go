package startupfence

import (
	"bufio"
	"strings"
	"testing"
)

func TestLocalProtocolRejectsOversizeAndUnknownFields(t *testing.T) {
	for _, line := range []string{`{"binding":"` + strings.Repeat("x", 32<<10) + `"}` + "\n", `{"unexpected":true}` + "\n", `{} {}` + "\n"} {
		var request sessionRequest
		if err := readLocalFrame(bufio.NewReaderSize(strings.NewReader(line), 16<<10), &request); err == nil {
			t.Fatal("accepted inadmissible local frame")
		}
	}
}
