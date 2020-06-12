package query

import (
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"

	"github.com/Syncano/pkg-go/database"
)

// Manager defines object manager.
type Manager struct {
	*Factory
	dbCtx database.DBContext
	curDB orm.DB
	dbGet func(database.DBContext) orm.DB
}

// NewManager creates and returns new manager.
func (q *Factory) NewManager(c database.DBContext) *Manager {
	return &Manager{
		Factory: q,
		dbCtx:   c,
		dbGet: func(c database.DBContext) orm.DB {
			return DB(q.db, c)
		},
	}
}

// NewTenantManager creates and returns new tenant manager.
func (q *Factory) NewTenantManager(c database.DBContext) *Manager {
	return &Manager{
		Factory: q,
		dbCtx:   c,
		dbGet: func(c database.DBContext) orm.DB {
			return TenantDB(q.db, c)
		},
	}
}

// DB returns standard database.
func (m *Manager) DB() orm.DB {
	if m.curDB != nil {
		return m.curDB
	}

	return m.dbGet(m.dbCtx)
}

// SetDB sets database.
func (m *Manager) SetDB(db orm.DB) {
	m.curDB = db
}

// Query returns all objects.
func (m *Manager) Query(o interface{}) *orm.Query {
	return m.DB().ModelContext(m.dbCtx.Request().Context(), o)
}

// Insert creates object.
func (m *Manager) Insert(model interface{}) error {
	db := m.DB()
	if _, err := db.ModelContext(m.dbCtx.Request().Context(), model).Insert(model); err != nil {
		return err
	}

	return m.db.ProcessModelSaveHook(m.dbCtx, db, true, model)
}

// Update updates object.
func (m *Manager) Update(model interface{}, fields ...string) error {
	db := m.DB()
	if _, err := db.ModelContext(m.dbCtx.Request().Context(), model).Column(fields...).WherePK().Update(); err != nil {
		return err
	}

	return m.db.ProcessModelSaveHook(m.dbCtx, db, false, model)
}

// Delete deletes object.
func (m *Manager) Delete(model interface{}) error {
	db := m.DB()
	if _, err := db.ModelContext(m.dbCtx.Request().Context(), model).Delete(); err != nil {
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
		tx, err = m.dbGet(m.dbCtx).(*pg.DB).Begin()
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
