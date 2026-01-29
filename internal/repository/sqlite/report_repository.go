package sqlite

import (
	"context"

	errwrap "github.com/pkg/errors"
	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/helper"
	"gorm.io/gorm"
)

type ReportRepository interface {
	GetSlowQueryReports(ctx context.Context, connectionID int64) ([]*entity.SlowQueryReport, error)
	SaveSlowQueryReports(ctx context.Context, connectionID int64, reports []*entity.SlowQueryReport) error
}

type reportRepo struct {
	db *gorm.DB
}

func NewReportRepository(db *gorm.DB) ReportRepository {
	return &reportRepo{db: db}
}

func (r *reportRepo) GetSlowQueryReports(ctx context.Context, connectionID int64) ([]*entity.SlowQueryReport, error) {
	funcName := "ReportRepository.GetSlowQueryReports"
	if err := helper.CheckDeadline(ctx); err != nil {
		return nil, errwrap.Wrap(err, funcName)
	}

	var reports []*entity.SlowQueryReport
	// Get latest ones
	err := r.db.WithContext(ctx).Where("connection_id = ?", connectionID).Order("max_duration_ms DESC").Find(&reports).Error
	if err != nil {
		return nil, errwrap.Wrap(err, funcName)
	}

	return reports, nil
}

func (r *reportRepo) SaveSlowQueryReports(ctx context.Context, connectionID int64, reports []*entity.SlowQueryReport) error {
	funcName := "ReportRepository.SaveSlowQueryReports"
	if err := helper.CheckDeadline(ctx); err != nil {
		return errwrap.Wrap(err, funcName)
	}

	return r.db.Transaction(func(tx *gorm.DB) error {
		// Delete existing entries for this connection
		if err := tx.Where("connection_id = ?", connectionID).Delete(&entity.SlowQueryReport{}).Error; err != nil {
			return errwrap.Wrap(err, funcName)
		}

		// Insert new ones
		if len(reports) > 0 {
			if err := tx.Create(reports).Error; err != nil {
				return errwrap.Wrap(err, funcName)
			}
		}
		return nil
	})
}
