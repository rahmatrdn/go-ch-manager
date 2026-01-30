package entity

import "time"

type FavoriteComparison struct {
	ID           int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	ConnectionID int64     `gorm:"index;not null" json:"connection_id"`
	Title        string    `gorm:"type:text;not null" json:"title"`
	Query1       string    `gorm:"type:text;not null" json:"query1"`
	Query2       string    `gorm:"type:text;not null" json:"query2"`
	CreatedAt    time.Time `gorm:"autoCreateTime" json:"created_at"`
}
