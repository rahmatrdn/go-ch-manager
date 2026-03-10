package main

import (
	"context"

	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/usecase"
)

// App struct holds the application dependencies
type App struct {
	ctx          context.Context
	connectionUC *usecase.ConnectionUsecase
	reportUC     usecase.ReportUsecase
}

// NewApp creates a new App application struct
func NewApp(connectionUC *usecase.ConnectionUsecase, reportUC usecase.ReportUsecase) *App {
	return &App{
		connectionUC: connectionUC,
		reportUC:     reportUC,
	}
}

// startup is called when the app starts
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
}

// Connection Management Methods

// GetAllConnections returns all saved connections
func (a *App) GetAllConnections() ([]*entity.CHConnection, error) {
	return a.connectionUC.GetAllConnections(a.ctx)
}

// CreateConnection creates a new connection
func (a *App) CreateConnection(conn *entity.CHConnection) error {
	return a.connectionUC.CreateConnection(a.ctx, conn)
}

// UpdateConnection updates an existing connection
func (a *App) UpdateConnection(id int64, conn *entity.CHConnection) error {
	return a.connectionUC.UpdateConnection(a.ctx, id, conn)
}

// GetConnectionStatus checks if connection is online
func (a *App) GetConnectionStatus(id int64) (string, error) {
	return a.connectionUC.GetConnectionStatus(a.ctx, id)
}

// GetServerInfo returns server info for a connection
func (a *App) GetServerInfo(id int64) (string, error) {
	return a.connectionUC.GetServerInfo(a.ctx, id)
}

// Database & Table Methods

// GetDatabases returns all databases for a connection
func (a *App) GetDatabases(id int64) ([]string, error) {
	return a.connectionUC.GetDatabases(a.ctx, id)
}

// GetTables returns all tables for a connection and optional database
func (a *App) GetTables(id int64, database string) ([]entity.TableMeta, error) {
	if database != "" {
		return a.connectionUC.GetTables(a.ctx, id, database)
	}
	return a.connectionUC.GetTables(a.ctx, id)
}

// GetSchema returns schema for a specific table
func (a *App) GetSchema(id int64, table string, database string) (*entity.TableSchema, string, error) {
	if database != "" {
		return a.connectionUC.GetSchema(a.ctx, id, table, database)
	}
	return a.connectionUC.GetSchema(a.ctx, id, table)
}

// Query Methods

// ExecuteQuery executes a SQL query and returns results
func (a *App) ExecuteQuery(id int64, query string) (*entity.QueryResult, error) {
	return a.connectionUC.ExecuteQuery(a.ctx, id, query)
}

// GetQueryHistory returns query history for a connection
func (a *App) GetQueryHistory(id int64) ([]*entity.QueryHistory, error) {
	return a.connectionUC.GetQueryHistory(a.ctx, id)
}

// CompareQueries compares two queries and returns their stats
func (a *App) CompareQueries(id int64, query1, query2 string) (*entity.CompareResult, error) {
	return a.connectionUC.CompareQueries(a.ctx, id, query1, query2)
}

// Configuration Methods

// GetConfigurationData returns configuration data for a connection
func (a *App) GetConfigurationData(id int64) (*entity.ConfigurationData, error) {
	return a.connectionUC.GetConfigurationData(a.ctx, id)
}

// Report Methods

// GetSlowQueries returns slow query reports
func (a *App) GetSlowQueries(connectionID int64) ([]*entity.SlowQueryReport, error) {
	reports, _, err := a.reportUC.GetTopSlowQueries(a.ctx, connectionID, false)
	return reports, err
}

// RefreshSlowQueries refreshes slow query data from ClickHouse
func (a *App) RefreshSlowQueries(connectionID int64) error {
	_, _, err := a.reportUC.GetTopSlowQueries(a.ctx, connectionID, true)
	return err
}
