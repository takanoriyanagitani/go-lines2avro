package enc

import (
	"context"
	"errors"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"
	la "github.com/takanoriyanagitani/go-lines2avro"
	. "github.com/takanoriyanagitani/go-lines2avro/util"
)

var (
	ErrInvalidSchema error = errors.New("invalid schema")
	ErrInvalidField  error = errors.New("invalid field")
)

func LinesToWriterRecordSchema(
	ctx context.Context,
	lines iter.Seq[string],
	w io.Writer,
	rs *ha.RecordSchema,
	opts ...ho.EncoderFunc,
) error {
	enc, e := ho.NewEncoderWithSchema(
		rs,
		w,
		opts...,
	)
	if nil != e {
		return e
	}
	defer enc.Close()

	var fields []*ha.Field = rs.Fields()
	if 1 != len(fields) {
		return ErrInvalidField
	}

	var field *ha.Field = fields[0]
	var lineName string = field.Name()

	buf := map[string]any{}

	for row := range lines {
		clear(buf)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		buf[lineName] = row

		e := enc.Encode(buf)
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

func LinesToWriterHamba(
	ctx context.Context,
	lines iter.Seq[string],
	w io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	switch typ := s.(type) {
	case *ha.RecordSchema:
		return LinesToWriterRecordSchema(
			ctx,
			lines,
			w,
			typ,
			opts...,
		)
	default:
		return ErrInvalidSchema
	}
}

func LinesToWriter(
	ctx context.Context,
	lines iter.Seq[string],
	w io.Writer,
	schema string,
	cfg la.EncodeConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}

	var opts []ho.EncoderFunc = ConfigToOpts(cfg)
	return LinesToWriterHamba(
		ctx,
		lines,
		w,
		parsed,
		opts...,
	)
}

func LinesToStdout(
	ctx context.Context,
	lines iter.Seq[string],
	schema string,
	cfg la.EncodeConfig,
) error {
	return LinesToWriter(
		ctx,
		lines,
		os.Stdout,
		schema,
		cfg,
	)
}

func LinesToStdoutDefault(
	ctx context.Context,
	lines iter.Seq[string],
	schema string,
) error {
	return LinesToStdout(ctx, lines, schema, la.EncodeConfigDefault)
}

func SchemaToLinesToStdoutDefault(
	schema string,
) func(iter.Seq[string]) IO[Void] {
	return func(lines iter.Seq[string]) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			return Empty, LinesToStdoutDefault(
				ctx,
				lines,
				schema,
			)
		}
	}
}

var DefaultSchemaToLinesToStdoutDefault func(
	iter.Seq[string],
) IO[Void] = SchemaToLinesToStdoutDefault(string(la.SchemaDefault))
