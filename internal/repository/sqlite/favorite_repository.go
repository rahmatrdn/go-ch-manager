package sqlite

import (
	"context"

	errwrap "github.com/pkg/errors"
	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/helper"
	"gorm.io/gorm"
)

type FavoriteRepository interface {
	Create(ctx context.Context, favorite *entity.FavoriteComparison) error
	FindAllByConnectionID(ctx context.Context, connectionID int64) ([]*entity.FavoriteComparison, error)
	FindByID(ctx context.Context, id int64) (*entity.FavoriteComparison, error)
	Delete(ctx context.Context, id int64) error
}

type favoriteRepository struct {
	db *gorm.DB
}

func NewFavoriteRepository(db *gorm.DB) FavoriteRepository {
	return &favoriteRepository{db: db}
}

func (r *favoriteRepository) Create(ctx context.Context, favorite *entity.FavoriteComparison) error {
	funcName := "FavoriteRepository.Create"
	if err := helper.CheckDeadline(ctx); err != nil {
		return errwrap.Wrap(err, funcName)
	}

	return r.db.WithContext(ctx).Create(favorite).Error
}

func (r *favoriteRepository) FindAllByConnectionID(ctx context.Context, connectionID int64) ([]*entity.FavoriteComparison, error) {
	funcName := "FavoriteRepository.FindAllByConnectionID"
	if err := helper.CheckDeadline(ctx); err != nil {
		return nil, errwrap.Wrap(err, funcName)
	}

	var favorites []*entity.FavoriteComparison
	err := r.db.WithContext(ctx).
		Where("connection_id = ?", connectionID).
		Order("created_at desc").
		Find(&favorites).Error

	if err != nil {
		return nil, errwrap.Wrap(err, funcName)
	}
	return favorites, nil
}

func (r *favoriteRepository) FindByID(ctx context.Context, id int64) (*entity.FavoriteComparison, error) {
	funcName := "FavoriteRepository.FindByID"
	if err := helper.CheckDeadline(ctx); err != nil {
		return nil, errwrap.Wrap(err, funcName)
	}

	var favorite entity.FavoriteComparison
	err := r.db.WithContext(ctx).
		First(&favorite, id).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, errwrap.Wrap(err, funcName)
	}
	return &favorite, nil
}

func (r *favoriteRepository) Delete(ctx context.Context, id int64) error {
	funcName := "FavoriteRepository.Delete"
	if err := helper.CheckDeadline(ctx); err != nil {
		return errwrap.Wrap(err, funcName)
	}

	return r.db.WithContext(ctx).Delete(&entity.FavoriteComparison{}, id).Error
}
