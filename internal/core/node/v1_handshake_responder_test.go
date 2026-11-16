package node

import (
	"bufio"
	"io"
	"strings"

	"github.com/piratecash/corsa/internal/core/protocol"
)

func readTestFrameLine(reader *bufio.Reader) (protocol.Frame, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return protocol.Frame{}, err
	}
	return protocol.ParseFrameLine(strings.TrimSpace(line))
}

func writeTestFrameLine(conn io.Writer, frame protocol.Frame) error {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return err
	}
	_, err = io.WriteString(conn, line)
	return err
}
