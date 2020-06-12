package database

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/go-pg/pg/v9"
	"go.uber.org/zap"

	"github.com/Syncano/pkg-go/log"
	"github.com/Syncano/pkg-go/util"
)

type key int

const (
	// KeySchema is used in Context as a key to describe Schema.
	KeySchema        key = iota
	dbConnRetries        = 10
	dbConnRetrySleep     = 250 * time.Millisecond
)

type DB struct {
	commonDB *pg.DB
	tenantDB *pg.DB

	// db hooks
	dbhookmu      sync.RWMutex
	commitHooks   map[*pg.Tx][]hookFunc
	rollbackHooks map[*pg.Tx][]hookFunc

	// model hooks
	modelhookmu     sync.RWMutex
	saveHooks       map[string][]SaveModelHookFunc
	deleteHooks     map[string][]DeleteModelHookFunc
	softDeleteHooks map[string][]SoftDeleteModelHookFunc
}

// DefaultDBOptions returns
func DefaultDBOptions() *pg.Options {
	return &pg.Options{
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var conn net.Conn

			return conn, util.Retry(dbConnRetries, dbConnRetrySleep, func() error {
				var (
					err error
				)
				d := net.Dialer{Timeout: 3 * time.Second}
				conn, err = d.DialContext(ctx, network, addr)
				return err
			})
		},
		PoolSize:    10,
		IdleTimeout: 5 * time.Minute,
		PoolTimeout: 30 * time.Second,
		MaxConnAge:  15 * time.Minute,
		MaxRetries:  1,
	}
}

// NewDB creates a database.
func NewDB(opts, instancesOpts *pg.Options, logger *log.Logger, debug bool) *DB {
	commonDB := initDB(opts, logger, debug)
	tenantDB := commonDB

	if instancesOpts.Addr != opts.Addr || instancesOpts.Database != opts.Database {
		tenantDB = initDB(instancesOpts, logger, debug)
	}

	return &DB{
		commonDB: commonDB,
		tenantDB: tenantDB,

		commitHooks:   make(map[*pg.Tx][]hookFunc),
		rollbackHooks: make(map[*pg.Tx][]hookFunc),

		saveHooks:       make(map[string][]SaveModelHookFunc),
		deleteHooks:     make(map[string][]DeleteModelHookFunc),
		softDeleteHooks: make(map[string][]SoftDeleteModelHookFunc),
	}
}

type debugHook struct {
	logger *zap.Logger
}

func (*debugHook) BeforeQuery(ctx context.Context, ev *pg.QueryEvent) (context.Context, error) {
	return ctx, nil
}

func (h *debugHook) AfterQuery(ctx context.Context, event *pg.QueryEvent) error {
	query, err := event.FormattedQuery()
	if err != nil {
		panic(err)
	}

	h.logger.Debug("Query",
		zap.String("query", query),
		zap.Duration("took", time.Since(event.StartTime)),
	)

	return nil
}

func initDB(opts *pg.Options, logger *log.Logger, debug bool) *pg.DB {
	db := pg.Connect(opts)

	if debug {
		db.AddQueryHook(&debugHook{logger: logger.Logger().WithOptions(zap.AddCallerSkip(8))})
	}

	return db
}

// DB returns database client.
func (d *DB) DB() *pg.DB {
	return d.commonDB
}

// TenantDB returns database client.
func (d *DB) TenantDB(schema string) *pg.DB {
	return d.tenantDB.WithParam("schema", pg.Ident(schema)).WithContext(context.WithValue(context.Background(), KeySchema, schema))
}

func (d *DB) Shutdown() error {
	if err := d.commonDB.Close(); err != nil {
		return err
	}

	if d.tenantDB != d.commonDB {
		return d.tenantDB.Close()
	}

	return nil
}

const ContextSchemaKey = "schema"

// GetDB returns base db for context.
func GetDB(db Databaser, c DBContext) *pg.DB {
	return db.DB()
}

// GetTenantDB returns base tenant db for context.
func GetTenantDB(db Databaser, c DBContext) *pg.DB {
	schema := c.Get(ContextSchemaKey).(string)
	return db.TenantDB(schema)
}
