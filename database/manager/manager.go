package manager

import (
	"context"

	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"

	"github.com/Syncano/pkg-go/v2/database"
)

// Manager defines object manager.
type Manager struct {
	db    *database.DB
	dbCtx database.DBContext
	curDB orm.DB
	dbGet func() orm.DB
}

// NewManager creates and returns new manager.
func NewManager(c database.DBContext, db *database.DB) *Manager {
	return &Manager{
		db:    db,
		dbCtx: c,
		dbGet: func() orm.DB {
			return db.DB()
		},
	}
}

// NewTenantManager creates and returns new tenant manager.
func NewTenantManager(c database.DBContext, db *database.DB) *Manager {
	return &Manager{
		db:    db,
		dbCtx: c,
		dbGet: func() orm.DB {
			return db.TenantDB(c.Schema())
		},
	}
}

// DB returns standard database.
func (m *Manager) DB() orm.DB {
	if m.curDB != nil {
		return m.curDB
	}

	return m.dbGet()
}

func (m *Manager) DBContext() database.DBContext {
	return m.dbCtx
}

// SetDB sets database.
func (m *Manager) SetDB(db orm.DB) {
	m.curDB = db
}

// Query returns all objects.
func (m *Manager) Query(o interface{}) *orm.Query {
	return m.QueryContext(context.Background(), o)
}

func (m *Manager) QueryContext(ctx context.Context, o interface{}) *orm.Query {
	return m.DB().ModelContext(ctx, o)
}

// Insert creates object.
func (m *Manager) Insert(model interface{}) error {
	return m.InsertContext(context.Background(), model)
}

func (m *Manager) InsertContext(ctx context.Context, model interface{}) error {
	db := m.DB()
	if _, err := db.ModelContext(ctx, model).Insert(model); err != nil {
		return err
	}

	return m.db.ProcessModelSaveHook(m.dbCtx, db, true, model)
}

// Update updates object.
func (m *Manager) Update(model interface{}, fields ...string) error {
	return m.UpdateContext(context.Background(), model, fields...)
}

func (m *Manager) UpdateContext(ctx context.Context, model interface{}, fields ...string) error {
	db := m.DB()
	if _, err := db.ModelContext(ctx, model).Column(fields...).WherePK().Update(); err != nil {
		return err
	}

	return m.db.ProcessModelSaveHook(m.dbCtx, db, false, model)
}

// Delete deletes object.
func (m *Manager) Delete(model interface{}) error {
	return m.DeleteContext(context.Background(), model)
}

func (m *Manager) DeleteContext(ctx context.Context, model interface{}) error {
	db := m.DB()
	if _, err := db.ModelContext(ctx, model).Delete(); err != nil {
		return err
	}

	return m.db.ProcessModelDeleteHook(m.dbCtx, db, model)
}

// RunInTransaction is an alias for DB function.
func (m *Manager) RunInTransaction(fn func(*pg.Tx) error) error {
	var (
		tx  *pg.Tx
		err error
	)

	if m.curDB == nil {
		tx, err = m.dbGet().(*pg.DB).Begin()
		if err != nil {
			return err
		}

		m.curDB = tx

		defer func() {
			m.curDB = nil
		}()
	}

	return m.db.RunTransactionWithHooks(tx, fn)
}
