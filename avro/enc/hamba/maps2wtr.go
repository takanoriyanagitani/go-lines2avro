package enc

import (
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"
	la "github.com/takanoriyanagitani/go-lines2avro"
	. "github.com/takanoriyanagitani/go-lines2avro/util"
)

func MapsToWriterHamba(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, e := ho.NewEncoderWithSchema(
		s,
		w,
		opts...,
	)
	if nil != e {
		return e
	}
	defer enc.Close()

	for row, e := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != e {
			return e
		}

		e = enc.Encode(row)
		if nil != e {
			return e
		}

		e = enc.Flush()
		if nil != e {
			return e
		}
	}

	return enc.Flush()
}

func CodecConv(c la.Codec) ho.CodecName {
	switch c {
	case la.CodecNull:
		return ho.Null
	case la.CodecDeflate:
		return ho.Deflate
	case la.CodecSnappy:
		return ho.Snappy
	case la.CodecZstd:
		return ho.ZStandard
	default:
		return ho.Null
	}
}

func ConfigToOpts(cfg la.EncodeConfig) []ho.EncoderFunc {
	var c ho.CodecName = CodecConv(cfg.Codec)
	return []ho.EncoderFunc{
		ho.WithBlockLength(cfg.BlockLength),
		ho.WithCodec(c),
	}
}

func MapsToWriter(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	schema string,
	cfg la.EncodeConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}

	var opts []ho.EncoderFunc = ConfigToOpts(cfg)
	return MapsToWriterHamba(
		ctx,
		m,
		w,
		parsed,
		opts...,
	)
}

func MapsToStdout(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
	cfg la.EncodeConfig,
) error {
	return MapsToWriter(
		ctx,
		m,
		os.Stdout,
		schema,
		cfg,
	)
}

func MapsToStdoutDefault(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
) error {
	return MapsToStdout(ctx, m, schema, la.EncodeConfigDefault)
}

func SchemaToMapsToStdoutDefault(
	schema string,
) func(iter.Seq2[map[string]any, error]) IO[Void] {
	return func(m iter.Seq2[map[string]any, error]) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			return Empty, MapsToStdoutDefault(
				ctx,
				m,
				schema,
			)
		}
	}
}
