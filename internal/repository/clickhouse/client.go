package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/helper"
)

type ClickHouseClient interface {
	Ping(ctx context.Context, conn *entity.CHConnection) error
	GetDatabases(ctx context.Context, conn *entity.CHConnection) ([]string, error)
	GetTables(ctx context.Context, conn *entity.CHConnection) ([]entity.TableMeta, error)
	GetCreateSQL(ctx context.Context, conn *entity.CHConnection, tableName string) (string, error)
	GetServerInfo(ctx context.Context, conn *entity.CHConnection) (string, error)
	GetSchema(ctx context.Context, conn *entity.CHConnection, tableName string) (*entity.TableSchema, error)
	ExecuteQueryWithStats(ctx context.Context, conn *entity.CHConnection, query string) (*entity.QueryStats, error)
	ExecuteQueryWithResults(ctx context.Context, conn *entity.CHConnection, query string) (*entity.QueryResult, error)
}

type clientImpl struct {
	conns map[string]driver.Conn
	mu    sync.RWMutex
}

func NewClickHouseClient() ClickHouseClient {
	return &clientImpl{
		conns: make(map[string]driver.Conn),
	}
}

func (c *clientImpl) getConnection(conn *entity.CHConnection) (driver.Conn, error) {
	// Create a unique key for the connection configuration
	key := fmt.Sprintf("%v|%s|%d|%s|%s|%v|%s", conn.ID, conn.Host, conn.Port, conn.Username, conn.Database, conn.UseSSL, conn.Protocol)

	c.mu.RLock()
	existingConn, ok := c.conns[key]
	c.mu.RUnlock()

	if ok {
		// Verify if connection is still alive
		if err := existingConn.Ping(context.Background()); err == nil {
			return existingConn, nil
		}
		// If dead, remove from pool and close (safely)
		_ = existingConn.Close()
		c.mu.Lock()
		delete(c.conns, key)
		c.mu.Unlock()
	}

	addr := fmt.Sprintf("%s:%d", conn.Host, conn.Port)

	options := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: conn.Database,
			Username: conn.Username,
			Password: conn.Password,
		},
		Protocol: clickhouse.Native, // Default to Native
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "go-ch-manager", Version: "0.1"},
			},
		},
		Debug: false,
	}

	if conn.Protocol == "http" {
		options.Protocol = clickhouse.HTTP
	}

	if conn.UseSSL {
		options.TLS = &tls.Config{
			InsecureSkipVerify: true, // For now, allow self-signed or just skip verify to avoid complex cert loading UI. User just wants to toggle SSL.
		}
	}

	newConn, err := clickhouse.Open(options)
	if err != nil {
		return nil, err
	}
	helper.DumpWithTitle(newConn, "newConn")

	// Verify new connection immediately
	if err := newConn.Ping(context.Background()); err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.conns[key] = newConn
	c.mu.Unlock()

	return newConn, nil
}

func (c *clientImpl) Ping(ctx context.Context, conn *entity.CHConnection) error {
	db, err := c.getConnection(conn)
	if err != nil {
		return err
	}

	if err := db.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return fmt.Errorf("clickhouse exception: [%d] %s", exception.Code, exception.Message)
		}
		return err
	}
	return nil
}

func (c *clientImpl) GetDatabases(ctx context.Context, conn *entity.CHConnection) ([]string, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	var dbs []string
	query := "SHOW DATABASES"
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		dbs = append(dbs, name)
	}

	return dbs, nil
}

func (c *clientImpl) GetTables(ctx context.Context, conn *entity.CHConnection) ([]entity.TableMeta, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	if conn.Database == "" {
		conn.Database = "default"
	}

	var tables []entity.TableMeta
	// Query system.tables to get engine type
	query := "SELECT name, engine FROM system.tables WHERE database = ?"

	rows, err := db.Query(ctx, query, conn.Database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, engine string
		if err := rows.Scan(&name, &engine); err != nil {
			return nil, err
		}
		tables = append(tables, entity.TableMeta{
			Name:   name,
			Engine: engine,
		})
	}

	return tables, nil
}

func (c *clientImpl) GetCreateSQL(ctx context.Context, conn *entity.CHConnection, tableName string) (string, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return "", err
	}

	if conn.Database == "" {
		conn.Database = "default"
	}

	// SHOW CREATE TABLE return format varies, usually it's the second column or just the statement
	// ClickHouse 'SHOW CREATE TABLE' returns a single row with 'statement' column mostly
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", conn.Database, tableName)

	rows, err := db.Query(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		var statement string
		// SHOW CREATE TABLE returns 1 column in recent versions
		if err := rows.Scan(&statement); err != nil {
			return "", err
		}
		return statement, nil
	}

	return "", fmt.Errorf("table not found")
}

func (c *clientImpl) GetServerInfo(ctx context.Context, conn *entity.CHConnection) (string, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return "", err
	}

	var version string
	if err := db.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return "", err
	}
	return version, nil
}

func (c *clientImpl) GetSchema(ctx context.Context, conn *entity.CHConnection, tableName string) (*entity.TableSchema, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	query := "SELECT name, type FROM system.columns WHERE table = ? AND database = ?"
	if conn.Database == "" {
		// If no database specified in connection, we might be in 'default' or relying on server default.
		// Safe bet: use 'default' or try to get current database.
		// For now let's assume 'default' if empty, or better, query without database filter but that's risky.
		// Let's assume the user provided database. If not, we default to 'default'.
		conn.Database = "default"
	}

	rows, err := db.Query(ctx, query, tableName, conn.Database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schema := &entity.TableSchema{
		Name:    tableName,
		Columns: []entity.TableSchemaColumn{},
	}

	for rows.Next() {
		var name, typeVal string
		if err := rows.Scan(&name, &typeVal); err != nil {
			return nil, err
		}
		schema.Columns = append(schema.Columns, entity.TableSchemaColumn{
			Name: name,
			Type: typeVal,
		})
	}

	return schema, nil
}

func (c *clientImpl) ExecuteQueryWithStats(ctx context.Context, conn *entity.CHConnection, query string) (*entity.QueryStats, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	queryID := uuid.New().String()

	// Context with QueryID
	ctxQuery := clickhouse.Context(ctx, clickhouse.WithQueryID(queryID))

	clickhouse.WithQueryID(queryID)
	helper.DumpWithTitle(queryID, "queryID")

	start := time.Now()
	// Execute main query
	rows, err := db.Query(ctxQuery, query)
	if err != nil {
		return nil, err
	}
	rows.Close() // Close immediately, we just want execution
	duration := time.Since(start).Milliseconds()

	// Flush logs to ensure data is written to system.query_log
	if err := db.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		// Log warning? For now just proceed, might miss data if not flushed
	}

	// Fetch stats from system.query_log
	// We wait a tiny bit? Ideally flush logs handles it.
	statsQuery := `
		SELECT
			query_duration_ms,
			read_rows,
			read_bytes,
			memory_usage,
			ProfileEvents['SelectedParts'] as parts,
			ProfileEvents['SelectedMarks'] as marks
		FROM system.query_log
		WHERE type = 'QueryFinish' 
			AND query_id = ? 
			AND query != 'SELECT displayName(), version(), revision(), timezone()'
		LIMIT 1
	`

	var (
		qDuration   uint64 // CH stores as UInt64
		readRows    uint64
		readBytes   uint64
		memoryUsage uint64
		parts       uint64
		marks       uint64
	)

	helper.DumpWithTitle(statsQuery, "statsQuery")
	helper.DumpWithTitle(queryID, "queryID")

	// Retry loop? just once for now.
	err = db.QueryRow(ctx, statsQuery, queryID).Scan(&qDuration, &readRows, &readBytes, &memoryUsage, &parts, &marks)
	if err != nil {
		// Fallback to client side timing if log not found immediately (async insert issue?)
		// But user wants log data. Return partially empty or error?
		// Let's return what we have (client side duration) and zeros if log fails.
		return &entity.QueryStats{
			ExecutionTimeMs: duration,
			RowsRead:        0, // Unknown from log
			BytesRead:       0,
			MemoryPeak:      0,
			PartsRead:       0,
		}, nil
	}

	return &entity.QueryStats{
		ExecutionTimeMs: int64(qDuration),
		RowsRead:        readRows,
		BytesRead:       readBytes,
		MemoryPeak:      memoryUsage,
		PartsRead:       parts,
	}, nil
}

func (c *clientImpl) ExecuteQueryWithResults(ctx context.Context, conn *entity.CHConnection, query string) (*entity.QueryResult, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	queryID := uuid.New().String()
	ctxQuery := clickhouse.Context(ctx, clickhouse.WithQueryID(queryID))

	start := time.Now()
	rows, err := db.Query(ctxQuery, query)
	if err != nil {
		return nil, err
	}
	// Get Columns
	columns := rows.Columns()
	result := &entity.QueryResult{
		Columns: columns,
		Rows:    make([]map[string]interface{}, 0),
	}

	// Dynamic Scan
	columnTypes := rows.ColumnTypes()

	for rows.Next() {
		valuePtrs := make([]interface{}, len(columns))
		for i, ct := range columnTypes {
			// ClickHouse native driver requires scanning into specific types
			// We use reflection to allocate a pointer to the type the driver expects
			valuePtrs[i] = reflect.New(ct.ScanType()).Interface()
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			rows.Close() // Ensure closed on error
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			// valuePtrs[i] is a pointer to the value, we need to dereference it
			val := reflect.ValueOf(valuePtrs[i]).Elem().Interface()
			rowMap[col] = val
		}
		result.Rows = append(result.Rows, rowMap)
	}

	// Close rows explicitly to signal query finish to server for logging
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	duration := time.Since(start).Milliseconds()

	// Flush logs
	_ = db.Exec(ctx, "SYSTEM FLUSH LOGS")

	// Get Stats
	statsQuery := `
		SELECT
			query_duration_ms,
			read_rows,
			read_bytes,
			memory_usage,
			ProfileEvents['SelectedParts'] as parts,
			ProfileEvents['SelectedMarks'] as marks
		FROM system.query_log
		WHERE type = 'QueryFinish' 
			AND query_id = ? 
			AND query != 'SELECT displayName(), version(), revision(), timezone()'
		LIMIT 1
	`
	stats := &entity.QueryStats{
		ExecutionTimeMs: duration, // Fallback
	}

	var (
		qDuration   uint64
		readRows    uint64
		readBytes   uint64
		memoryUsage uint64
		parts       uint64
		marks       uint64
	)

	err = db.QueryRow(ctx, statsQuery, queryID).Scan(&qDuration, &readRows, &readBytes, &memoryUsage, &parts, &marks)
	if err == nil {
		stats.ExecutionTimeMs = int64(qDuration)
		stats.RowsRead = readRows
		stats.BytesRead = readBytes
		stats.MemoryPeak = memoryUsage
		stats.PartsRead = parts
		stats.MarksRead = marks
	}

	result.Stats = stats
	return result, nil
}
