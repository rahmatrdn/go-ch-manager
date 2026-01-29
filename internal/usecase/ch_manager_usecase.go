package usecase

import (
	"context"
	"time"

	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/repository/clickhouse"
	"github.com/rahmatrdn/go-ch-manager/internal/repository/sqlite"
)

type ConnectionUsecase struct {
	repo     sqlite.ConnectionRepository
	chClient clickhouse.ClickHouseClient
}

func NewConnectionUsecase(repo sqlite.ConnectionRepository, chClient clickhouse.ClickHouseClient) *ConnectionUsecase {
	return &ConnectionUsecase{
		repo:     repo,
		chClient: chClient,
	}
}

func (u *ConnectionUsecase) CreateConnection(ctx context.Context, conn *entity.CHConnection) error {
	conn.CreatedAt = time.Now()
	conn.UpdatedAt = time.Now()
	// Optionally test connection before saving?
	// err := u.chClient.Ping(ctx, conn)
	// if err != nil { return err }
	return u.repo.Create(ctx, conn)
}

func (u *ConnectionUsecase) UpdateConnection(ctx context.Context, id int64, conn *entity.CHConnection) error {
	existing, err := u.repo.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if existing == nil {
		// return specific not found error or generic
		return nil // Should probably return error, but keeping simple for now
	}

	conn.ID = id
	conn.UpdatedAt = time.Now()
	conn.CreatedAt = existing.CreatedAt // Preserve created_at

	// Optional: validate connection
	return u.repo.Update(ctx, conn)
}

func (u *ConnectionUsecase) GetAllConnections(ctx context.Context) ([]*entity.CHConnection, error) {
	return u.repo.FindAll(ctx)
}

func (u *ConnectionUsecase) GetConnectionStatus(ctx context.Context, id int64) (string, error) {
	conn, err := u.repo.FindByID(ctx, id)
	if err != nil {
		return "Error", err
	}
	if conn == nil {
		return "Not Found", nil
	}

	// err = u.chClient.Ping(ctx, conn)

	// if err != nil {
	// 	return "Offline", nil
	// }
	return "Online", nil
}

func (u *ConnectionUsecase) GetServerInfo(ctx context.Context, id int64) (string, error) {
	conn, err := u.repo.FindByID(ctx, id)
	if err != nil {
		return "", err
	}
	if conn == nil {
		return "", nil
	}
	return u.chClient.GetServerInfo(ctx, conn)
}

func (u *ConnectionUsecase) GetDatabases(ctx context.Context, id int64) ([]string, error) {
	conn, err := u.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, nil
	}
	return u.chClient.GetDatabases(ctx, conn)
}

func (u *ConnectionUsecase) GetTables(ctx context.Context, id int64, db ...string) ([]entity.TableMeta, error) {
	conn, err := u.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, nil // Or error not found
	}

	// Override database if provided
	if len(db) > 0 && db[0] != "" {
		conn.Database = db[0]
	}

	return u.chClient.GetTables(ctx, conn)
}

func (u *ConnectionUsecase) GetSchema(ctx context.Context, id int64, table string, db ...string) (*entity.TableSchema, string, error) {
	conn, err := u.repo.FindByID(ctx, id)
	if err != nil {
		return nil, "", err
	}
	if conn == nil {
		return nil, "", nil
	}

	// Override database if provided
	if len(db) > 0 && db[0] != "" {
		conn.Database = db[0]
	}

	schema, err := u.chClient.GetSchema(ctx, conn, table)
	if err != nil {
		return nil, "", err
	}

	createSQL, err := u.chClient.GetCreateSQL(ctx, conn, table)
	if err != nil {
		// Non-critical if create SQL fails?
		createSQL = "-- Failed to fetch create SQL"
	}

	return schema, createSQL, nil
}

func (u *ConnectionUsecase) CompareQueries(ctx context.Context, id int64, query1, query2 string) (*entity.CompareResult, error) {
	conn, err := u.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, nil // Or error not found
	}

	stats1, err := u.chClient.ExecuteQueryWithStats(ctx, conn, query1)
	if err != nil {
		return nil, err
	}

	stats2, err := u.chClient.ExecuteQueryWithStats(ctx, conn, query2)
	if err != nil {
		return nil, err
	}

	return &entity.CompareResult{
		Query1Stats: stats1,
		Query2Stats: stats2,
	}, nil
}

func (u *ConnectionUsecase) ExecuteQuery(ctx context.Context, id int64, query string) (*entity.QueryResult, error) {
	conn, err := u.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, nil // Or return not found error
	}

	return u.chClient.ExecuteQueryWithResults(ctx, conn, query)
}

func (u *ConnectionUsecase) GetConfigurationData(ctx context.Context, id int64) (*entity.ConfigurationData, error) {
	conn, err := u.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, nil
	}

	data := &entity.ConfigurationData{}

	// Fetch data sequentially (could be parallelized)
	info, err := u.chClient.GetClusterConfig(ctx, conn)
	if info != nil {
		data.ClusterInfo = *info
	}
	// We ignore err for ClusterInfo because we want to show partial data (Host/Port) even if connection fails

	if settings, err := u.chClient.GetSettings(ctx, conn); err == nil {
		data.Settings = settings
	}

	if users, err := u.chClient.GetUsers(ctx, conn); err == nil {
		data.Users = users
	}

	if roles, err := u.chClient.GetRoles(ctx, conn); err == nil {
		data.Roles = roles
	}

	if policies, disks, err := u.chClient.GetStoragePolicies(ctx, conn); err == nil {
		data.StoragePolicies = policies
		data.Disks = disks
	}

	if stats, err := u.chClient.GetProcessStats(ctx, conn); err == nil {
		data.Processes = *stats
	}

	if logCfg, err := u.chClient.GetLogConfig(ctx, conn); err == nil {
		data.LogConfig = *logCfg
	}

	return data, nil
}
