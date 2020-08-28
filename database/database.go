package database

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-pg/pg/v9"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"

	"github.com/Syncano/pkg-go/v2/log"
	"github.com/Syncano/pkg-go/v2/util"
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

type Options struct {
	pg.Options

	StatementTimeout time.Duration
	Host             string
	Port             string
}

// DefaultDBOptions returns
func DefaultDBOptions() *Options {
	return &Options{
		Options: pg.Options{
			PoolSize:    10,
			IdleTimeout: 5 * time.Minute,
			PoolTimeout: 30 * time.Second,
			MaxConnAge:  15 * time.Minute,
			MaxRetries:  1,
		},
	}
}

func (o *Options) PGOptions() *pg.Options {
	opts := o.Options

	if o.Host != "" {
		port := o.Port

		if port == "" {
			port = "5432"
		}

		opts.Addr = fmt.Sprintf("%s:%s", o.Host, port)
	}

	if opts.Dialer == nil {
		opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			var conn net.Conn

			return conn, util.Retry(dbConnRetries, dbConnRetrySleep, func() error {
				var (
					err error
				)
				d := net.Dialer{Timeout: o.DialTimeout, KeepAlive: 5 * time.Minute}
				conn, err = d.DialContext(ctx, network, addr)
				return err
			})
		}
	}

	if opts.OnConnect == nil && o.StatementTimeout != 0 {
		opts.OnConnect = func(conn *pg.Conn) error {
			_, err := conn.Exec("SET statement_timeout = {}", o.StatementTimeout/time.Microsecond)
			return err
		}
	}

	return &opts
}

// NewDB creates a database.
func NewDB(opts, instancesOpts *Options, logger *log.Logger, debug bool) *DB {
	commonDB := initDB(opts.PGOptions(), logger, debug)
	tenantDB := commonDB

	if instancesOpts != nil && !cmp.Equal(instancesOpts, opts) {
		tenantDB = initDB(instancesOpts.PGOptions(), logger, debug)
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
