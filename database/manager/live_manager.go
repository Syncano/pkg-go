package manager

import (
	"github.com/go-pg/pg/v9/orm"

	"github.com/Syncano/pkg-go/v2/database"
)

// LiveManager defines a manager with live functionality.
type LiveManager struct {
	*Manager
}

// NewLiveManager creates and returns new live manager.
func NewLiveManager(c database.DBContext, db *database.DB) *LiveManager {
	return &LiveManager{Manager: NewManager(c, db)}
}

// NewLiveTenantManager creates and returns new live tenant manager.
func NewLiveTenantManager(c database.DBContext, db *database.DB) *LiveManager {
	return &LiveManager{Manager: NewTenantManager(c, db)}
}

// Query returns only alive objects.
func (m *LiveManager) Query(o interface{}) *orm.Query {
	return m.DB().ModelContext(m.dbCtx.Context(), o).Where("?TableAlias._is_live IS TRUE")
}

// All returns all objects, irrelevant if they are alive or not.
func (m *LiveManager) All(o interface{}) *orm.Query {
	return m.DB().ModelContext(m.dbCtx.Context(), o)
}

// Dead returns dead objects.
func (m *LiveManager) Dead(o interface{}) *orm.Query {
	return m.DB().ModelContext(m.dbCtx.Context(), o).Where("?TableAlias._is_live IS NULL")
}

// Delete is a soft delete for live objects.
func (m *LiveManager) Delete(model interface{}) error {
	db := m.DB()
	if _, err := db.ModelContext(m.dbCtx.Context(), model).WherePK().Set("_is_live = ?", false).Update(); err != nil {
		return err
	}

	return m.db.ProcessModelSoftDeleteHook(m.dbCtx, db, model)
}

// HardDelete deletes object.
func (m *LiveManager) HardDelete(model interface{}) error {
	return m.Manager.Delete(model)
}
