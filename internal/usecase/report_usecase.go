package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/repository/clickhouse"
	"github.com/rahmatrdn/go-ch-manager/internal/repository/sqlite"
)

type ReportUsecase interface {
	GetTopSlowQueries(ctx context.Context, connectionID int64, forceRefresh bool) ([]*entity.SlowQueryReport, *time.Time, error)
}

type reportUsecase struct {
	reportRepo     sqlite.ReportRepository
	connectionRepo sqlite.ConnectionRepository
	chClient       clickhouse.ClickHouseClient
}

func NewReportUsecase(
	reportRepo sqlite.ReportRepository,
	connectionRepo sqlite.ConnectionRepository,
	chClient clickhouse.ClickHouseClient,
) ReportUsecase {
	return &reportUsecase{
		reportRepo:     reportRepo,
		connectionRepo: connectionRepo,
		chClient:       chClient,
	}
}

func (u *reportUsecase) GetTopSlowQueries(ctx context.Context, connectionID int64, forceRefresh bool) ([]*entity.SlowQueryReport, *time.Time, error) {
	// 1. Check SQLite if not forceRefresh
	if !forceRefresh {
		existing, err := u.reportRepo.GetSlowQueryReports(ctx, connectionID)
		if err != nil {
			return nil, nil, err
		}
		if len(existing) > 0 {
			// Find max UpdatedAt or CreatedAt as refresh time
			lastRef := existing[0].CreatedAt
			// Assuming all rows are inserted at once, picking one is enough.
			// Or we could scan for max.
			return existing, &lastRef, nil
		}
	}

	// 2. Fetch Connection Config
	conn, err := u.connectionRepo.FindByID(ctx, connectionID)
	if err != nil {
		return nil, nil, err
	}
	if conn == nil {
		return nil, nil, fmt.Errorf("connection not found")
	}

	// 3. Execute Query on ClickHouse
	query := `
SELECT
    initial_user                               AS executed_by,
    any(query)                             AS sample_query,
    normalizeQuery(query)                      AS query_normalized,
    count()                                    AS executions,
    round(avg(query_duration_ms), 2)           AS avg_duration_ms,
    quantileTDigest(0.95)(query_duration_ms)   AS p95_duration_ms,
    max(query_duration_ms)                     AS max_duration_ms,
    sum(read_rows)                             AS total_rows_read,
    sum(read_bytes)                            AS total_bytes_read
FROM system.query_log
WHERE
    event_time >= now() - INTERVAL 24 HOUR
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND is_initial_query = 1
GROUP BY
    executed_by,
    query_normalized
ORDER BY max_duration_ms DESC
LIMIT 20;
`
	res, err := u.chClient.ExecuteQueryWithResults(ctx, conn, query)
	if err != nil {
		// If fails, maybe return existing cache if available?
		// For now, return error.
		return nil, nil, err
	}

	// 4. Map Results
	var reports []*entity.SlowQueryReport
	now := time.Now()

	for _, row := range res.Rows {
		report := &entity.SlowQueryReport{
			ConnectionID:    connectionID,
			ExecutedBy:      getString(row["executed_by"]),
			SampleQuery:     getString(row["sample_query"]),
			QueryNormalized: getString(row["query_normalized"]),
			Executions:      getUint64(row["executions"]),
			AvgDurationMs:   getFloat64(row["avg_duration_ms"]),
			P95DurationMs:   getFloat64(row["p95_duration_ms"]),
			MaxDurationMs:   getFloat64(row["max_duration_ms"]),
			TotalRowsRead:   getUint64(row["total_rows_read"]),
			TotalBytesRead:  getUint64(row["total_bytes_read"]),
			LastRefresh:     now,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		reports = append(reports, report)
	}

	// 5. Save to SQLite
	if err := u.reportRepo.SaveSlowQueryReports(ctx, connectionID, reports); err != nil {
		return nil, nil, err
	}

	return reports, &now, nil
}

// Helpers for type assertion (ClickHouse driver can return various types)
func getString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func getUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case int64:
		return uint64(n)
	case float64:
		return uint64(n)
	}
	return 0
}

func getFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	}
	return 0
}
