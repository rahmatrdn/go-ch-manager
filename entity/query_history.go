package entity

import "time"

type QueryHistory struct {
	ID           int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	ConnectionID int64     `gorm:"index;not null" json:"connection_id"`
	Query        string    `gorm:"type:text;not null" json:"query"`
	CreatedAt    time.Time `gorm:"autoCreateTime" json:"created_at"`
}
