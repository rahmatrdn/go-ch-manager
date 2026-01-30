package entity

import "time"

type CHConnection struct {
	ID         int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	Name       string `json:"name" gorm:"type:varchar(255);not null"`
	Host       string `json:"host" gorm:"type:varchar(255);not null"`
	Port       int    `json:"port" gorm:"not null"`
	Username   string `json:"username" gorm:"type:varchar(255)"`
	Password   string `json:"password" gorm:"type:varchar(255)"`
	Database   string `json:"database" gorm:"type:varchar(255)"`
	Protocol   string `json:"protocol" gorm:"type:varchar(10);default:'native'"`
	UseSSL     bool   `json:"use_ssl" gorm:"default:false"`
	ServerInfo string `json:"server_info" gorm:"type:text"`
	Label      string `json:"label" gorm:"type:varchar(20);default:'DEVELOPMENT'"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type TableSchema struct {
	Name     string              `json:"name"`
	Database string              `json:"database"`
	Columns  []TableSchemaColumn `json:"columns"`
}

type TableSchemaColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type TableMeta struct {
	Name   string `json:"name"`
	Engine string `json:"engine"`
}

type QueryStats struct {
	ExecutionTimeMs int64  `json:"execution_time_ms"`
	RowsRead        uint64 `json:"rows_read"`
	BytesRead       uint64 `json:"bytes_read"`
	MemoryPeak      uint64 `json:"memory_peak"`
	PartsRead       uint64 `json:"parts_read"`
	MarksRead       uint64 `json:"marks_read"`
}

type QueryResult struct {
	Columns []string                 `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
	Stats   *QueryStats              `json:"stats"`
}

type CompareResult struct {
	Query1Stats *QueryStats `json:"query1_stats"`
	Query2Stats *QueryStats `json:"query2_stats"`
}
