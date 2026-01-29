package sqlite

import (
	"context"

	errwrap "github.com/pkg/errors"
	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/helper"
	"gorm.io/gorm"
)

type QueryHistoryRepository interface {
	Create(ctx context.Context, history *entity.QueryHistory) error
	FindByConnectionID(ctx context.Context, connectionID int64, limit int) ([]*entity.QueryHistory, error)
	Prune(ctx context.Context, connectionID int64, maxLimit int) error
}

type QueryHistory struct {
	db *gorm.DB
}

func NewQueryHistoryRepository(db *gorm.DB) *QueryHistory {
	return &QueryHistory{db: db}
}

func (r *QueryHistory) Create(ctx context.Context, history *entity.QueryHistory) error {
	funcName := "QueryHistoryRepository.Create"
	if err := helper.CheckDeadline(ctx); err != nil {
		return errwrap.Wrap(err, funcName)
	}

	return r.db.WithContext(ctx).Create(history).Error
}

func (r *QueryHistory) FindByConnectionID(ctx context.Context, connectionID int64, limit int) ([]*entity.QueryHistory, error) {
	funcName := "QueryHistoryRepository.FindByConnectionID"
	if err := helper.CheckDeadline(ctx); err != nil {
		return nil, errwrap.Wrap(err, funcName)
	}

	var histories []*entity.QueryHistory
	// Order by CreatedAt DESC to get latest queries
	err := r.db.WithContext(ctx).
		Where("connection_id = ?", connectionID).
		Order("created_at desc").
		Limit(limit).
		Find(&histories).Error

	if err != nil {
		return nil, errwrap.Wrap(err, funcName)
	}
	return histories, nil
}

func (r *QueryHistory) Prune(ctx context.Context, connectionID int64, maxLimit int) error {
	funcName := "QueryHistoryRepository.Prune"
	if err := helper.CheckDeadline(ctx); err != nil {
		return errwrap.Wrap(err, funcName)
	}

	// Subquery to find IDs to keep
	// SQLite supports row_number or limit/offset.
	// We want to delete entries where ID NOT IN (SELECT ID FROM ... ORDER BY created_at DESC LIMIT maxLimit)

	// Since GORM and SQLite version variation, a safe approach is:
	// Find the N-th record's timestamp or ID, and delete everything older/before.

	// OR using subquery delete:
	// DELETE FROM query_histories WHERE connection_id = ? AND id NOT IN (SELECT id FROM query_histories WHERE connection_id = ? ORDER BY created_at DESC LIMIT ?)

	return r.db.WithContext(ctx).
		Where("connection_id = ? AND id NOT IN (?)", connectionID,
			r.db.Model(&entity.QueryHistory{}).
				Select("id").
				Where("connection_id = ?", connectionID).
				Order("created_at desc").
				Limit(maxLimit),
		).
		Delete(&entity.QueryHistory{}).Error
}
