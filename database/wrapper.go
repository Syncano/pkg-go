package database

import (
	"context"
)

type wrappedContext struct {
	wrapped      interface{}
	ctx          context.Context
	schemaGetter func() string
}

var _ DBContext = (*wrappedContext)(nil)

func (w *wrappedContext) Schema() string {
	return w.schemaGetter()
}

func (w *wrappedContext) Context() context.Context {
	return w.ctx
}

func (w *wrappedContext) Unwrap() interface{} {
	return w.wrapped
}

func WrapContext(ctx context.Context, data interface{}) DBContext {
	return WrapContextWithSchema(ctx, "public", data)
}

func WrapContextWithSchema(ctx context.Context, schema string, data interface{}) DBContext {
	return WrapContextWithSchemaGetter(ctx, func() string { return schema }, data)
}

func WrapContextWithSchemaGetter(ctx context.Context, schemaGetter func() string, data interface{}) DBContext {
	return &wrappedContext{
		ctx:          ctx,
		schemaGetter: schemaGetter,
		wrapped:      data,
	}
}
