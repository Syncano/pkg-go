package database

import "context"

type wrappedContext struct {
	context.Context
	schemaGetter func() string
}

func (w *wrappedContext) Schema() string {
	return w.schemaGetter()
}

func WrapContext(ctx context.Context) DBContext {
	return WrapContextWithSchema(ctx, "public")
}

func WrapContextWithSchema(ctx context.Context, schema string) DBContext {
	return WrapContextWithSchemaGetter(ctx, func() string { return schema })
}

func WrapContextWithSchemaGetter(ctx context.Context, schemaGetter func() string) DBContext {
	return &wrappedContext{
		Context:      ctx,
		schemaGetter: schemaGetter,
	}
}
