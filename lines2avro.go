package lines2avro

import (
	"bufio"
	_ "embed"
	"io"
	"iter"
)

type Schema string

//go:embed simple-schema.avsc
var schemaDefault string

var SchemaDefault Schema = Schema(schemaDefault)

func ReaderToLines(rdr io.Reader) iter.Seq[string] {
	return func(yield func(string) bool) {
		var s *bufio.Scanner = bufio.NewScanner(rdr)

		for s.Scan() {
			var line string = s.Text()
			if !yield(line) {
				return
			}
		}
	}
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
