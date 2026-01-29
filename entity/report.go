package entity

import (
	"time"
)

// SlowQueryReport represents a row in the Top 10 Slow Queries report
type SlowQueryReport struct {
	ID              int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	ConnectionID    int64     `gorm:"index" json:"connection_id"`
	ExecutedBy      string    `json:"executed_by"`
	SampleQuery     string    `json:"sample_query"`
	QueryNormalized string    `json:"query_normalized"`
	Executions      uint64    `json:"executions"`
	AvgDurationMs   float64   `json:"avg_duration_ms"`
	P95DurationMs   float64   `json:"p95_duration_ms"`
	MaxDurationMs   float64   `json:"max_duration_ms"`
	TotalRowsRead   uint64    `json:"total_rows_read"`
	TotalBytesRead  uint64    `json:"total_bytes_read"`
	LastRefresh     time.Time `json:"last_refresh"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// TableName overrides the table name used by User to `slow_query_reports`
func (SlowQueryReport) TableName() string {
	return "slow_query_reports"
}
