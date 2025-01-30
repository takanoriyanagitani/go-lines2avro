package main

import (
	"context"
	"iter"
	"log"
	"os"

	la "github.com/takanoriyanagitani/go-lines2avro"
	eh "github.com/takanoriyanagitani/go-lines2avro/avro/enc/hamba"
	. "github.com/takanoriyanagitani/go-lines2avro/util"
)

var stdin2lines IO[iter.Seq[string]] = Of(la.ReaderToLines(os.Stdin))

var lines2avro2stdout func(iter.Seq[string]) IO[Void] = eh.
	DefaultSchemaToLinesToStdoutDefault

var stdin2lines2avro2stdout IO[Void] = Bind(
	stdin2lines,
	lines2avro2stdout,
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2lines2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
