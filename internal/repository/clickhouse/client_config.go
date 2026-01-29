package clickhouse

import (
	"context"
	"strings"
	"time"

	"github.com/rahmatrdn/go-ch-manager/entity"
)

// client_config.go implements configuration related methods for clientImpl

func (c *clientImpl) GetClusterConfig(ctx context.Context, conn *entity.CHConnection) (*entity.ClusterInfo, error) {
	// Always initialize info with static connection details first
	info := &entity.ClusterInfo{
		Host:            conn.Host,
		Port:            conn.Port,
		Protocol:        conn.Protocol,
		DatabaseDefault: conn.Database,
		ReadWriteMode:   true, // Assuming RW for now
	}

	db, err := c.getConnection(conn)
	if err != nil {
		// Return partial info even if connection fails (so UI shows something)
		// But also return error so caller knows
		return info, err
	}

	// Get basic version and time info
	// Try most complete query first
	query := "SELECT version(), uptime(), timezone(), displayName()"
	var version, timezone, displayName string
	var uptime uint64

	if err := db.QueryRow(ctx, query).Scan(&version, &uptime, &timezone, &displayName); err != nil {
		// Fallback: Try without displayName (older versions)
		if err2 := db.QueryRow(ctx, "SELECT version(), uptime(), timezone()").Scan(&version, &uptime, &timezone); err2 == nil {
			displayName = "ClickHouse Server"
		} else {
			// If both fail, we just log/ignore and keep zero values for version etc.
			// We don't return nil, err2 here anymore to ensure at least Host/Port are shown.
		}
	}
	info.Version = version
	info.Uptime = int64(uptime)
	info.Timezone = timezone
	info.DisplayName = displayName

	// Get Cluster info from system.clusters
	// User requested specific query:
	// SELECT cluster, countDistinct(shard_num) AS shards, countDistinct(replica_num) AS replicas FROM system.clusters GROUP BY cluster
	clusterQuery := "SELECT cluster, countDistinct(shard_num) AS shards, countDistinct(replica_num) AS replicas FROM system.clusters GROUP BY cluster LIMIT 1"

	rows, err := db.Query(ctx, clusterQuery)
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			var name string
			var shards, replicas uint64 // countDistinct returns UInt64 usually
			if err := rows.Scan(&name, &shards, &replicas); err == nil {
				info.ClusterName = name
				info.Shards = int(shards)
				info.Replicas = int(replicas)
			}
		} else {
			// No rows returned from system.clusters -> likely single node
			info.ClusterName = "Single Node (No Cluster Configured)"
			info.Shards = 1
			info.Replicas = 1
		}
	} else {
		// Error querying system.clusters, fallback to single node
		info.ClusterName = "Single Node (Query Failed)"
		info.Shards = 1
		info.Replicas = 1
	}

	return info, nil
}

func (c *clientImpl) GetSettings(ctx context.Context, conn *entity.CHConnection) ([]entity.CHSetting, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	query := "SELECT name, value, changed, description, type, readonly FROM system.settings ORDER BY name"
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var settings []entity.CHSetting
	for rows.Next() {
		var s entity.CHSetting
		var changed uint8
		if err := rows.Scan(&s.Name, &s.Value, &changed, &s.Description, &s.Type, &s.Readonly); err != nil {
			continue // Skip error rows?
		}
		s.Changed = changed == 1
		settings = append(settings, s)
	}
	return settings, nil
}

func (c *clientImpl) GetUsers(ctx context.Context, conn *entity.CHConnection) ([]entity.CHUser, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	// Check if system.users exists (modern CH)
	query := "SELECT name, id, storage, auth_type, host_ip, default_roles, default_database, profiles, quotas FROM system.users"
	rows, err := db.Query(ctx, query)
	if err != nil {
		// Fallback for very old versions or different access
		return []entity.CHUser{}, nil
	}
	defer rows.Close()

	var users []entity.CHUser
	for rows.Next() {
		var u entity.CHUser
		var defaultRoles, profiles, quotas []string
		// ClickHouse arrays are returned as []string or similar depending on driver
		if err := rows.Scan(&u.Name, &u.ID, &u.Storage, &u.AuthType, &u.HostIP, &defaultRoles, &u.DefaultDatabase, &profiles, &quotas); err != nil {
			// Try without scanning arrays if driver fails?
			// Assuming driver handles []string
			continue
		}

		if len(profiles) > 0 {
			u.Profile = strings.Join(profiles, ", ")
		}
		if len(quotas) > 0 {
			u.Quota = strings.Join(quotas, ", ")
		}
		u.DefaultRoles = defaultRoles
		users = append(users, u)
	}
	return users, nil
}

func (c *clientImpl) GetRoles(ctx context.Context, conn *entity.CHConnection) ([]entity.CHRole, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	query := "SELECT name, id, storage FROM system.roles"
	rows, err := db.Query(ctx, query)
	if err != nil {
		return []entity.CHRole{}, nil
	}
	defer rows.Close()

	var roles []entity.CHRole
	for rows.Next() {
		var r entity.CHRole
		if err := rows.Scan(&r.Name, &r.ID, &r.Storage); err != nil {
			continue
		}
		roles = append(roles, r)
	}
	return roles, nil
}

func (c *clientImpl) GetStoragePolicies(ctx context.Context, conn *entity.CHConnection) ([]entity.StoragePolicy, []entity.Disk, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, nil, err
	}

	// Helper to get Policies
	var policies []entity.StoragePolicy
	pRows, err := db.Query(ctx, "SELECT policy_name, volume_name, disks, move_factor, keep_free_space_bytes FROM system.storage_policies")
	if err == nil {
		defer pRows.Close()
		// Map to aggregate volumes by policy
		policyMap := make(map[string]*entity.StoragePolicy)

		for pRows.Next() {
			var pName, vName string
			var disks []string
			var moveFactor float64
			var keepFree uint64
			pRows.Scan(&pName, &vName, &disks, &moveFactor, &keepFree)

			if _, ok := policyMap[pName]; !ok {
				policyMap[pName] = &entity.StoragePolicy{
					Name:               pName,
					MoveFactor:         moveFactor,
					KeepFreeSpaceBytes: keepFree,
					Volumes:            []entity.StorageVolume{},
				}
			}
			policyMap[pName].Volumes = append(policyMap[pName].Volumes, entity.StorageVolume{
				Name:  vName,
				Disks: disks,
			})
		}
		for _, p := range policyMap {
			policies = append(policies, *p)
		}
	}

	// Helper to get Disks
	var disks []entity.Disk
	dRows, err := db.Query(ctx, "SELECT name, path, free_space, total_space, keep_free_space, type FROM system.disks")
	if err == nil {
		defer dRows.Close()
		for dRows.Next() {
			var d entity.Disk
			dRows.Scan(&d.Name, &d.Path, &d.FreeSpace, &d.TotalSpace, &d.KeepFreeSpace, &d.Type)
			disks = append(disks, d)
		}
	}

	return policies, disks, nil
}

func (c *clientImpl) GetProcessStats(ctx context.Context, conn *entity.CHConnection) (*entity.ProcessStats, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	stats := &entity.ProcessStats{}

	// Query Metrics
	rows, err := db.Query(ctx, "SELECT metric, value FROM system.metrics WHERE metric IN ('Query', 'BackgroundMerges', 'BackgroundFetches', 'MemoryTracking')")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var metric string
			var value int64
			rows.Scan(&metric, &value)
			switch metric {
			case "Query":
				stats.QueriesInProgress = int(value)
			case "BackgroundMerges":
				stats.BackgroundMerges = int(value)
			case "BackgroundFetches":
				stats.BackgroundFetches = int(value)
			case "MemoryTracking":
				stats.MemoryTracking = value
			}
		}
	}

	return stats, nil
}

func (c *clientImpl) GetLogConfig(ctx context.Context, conn *entity.CHConnection) (*entity.LogConfig, error) {
	db, err := c.getConnection(conn)
	if err != nil {
		return nil, err
	}

	cfg := &entity.LogConfig{}

	// Check query_log settings
	// We can check system.settings or try to query system.query_log to see range

	// 1. Check if enabled
	// query_log is usually enabled via config.xml, hard to check via SQL settings sometimes if it's not a user setting.
	// But we can check `log_queries` setting.
	var logQueries uint8
	if err := db.QueryRow(ctx, "SELECT value FROM system.settings WHERE name = 'log_queries'").Scan(&logQueries); err == nil {
		cfg.QueryLog.Enabled = logQueries == 1
	} else {
		// Default to true if cannot check? Or false.
		cfg.QueryLog.Enabled = true // Assumption
	}

	// 2. Check flush interval
	var flushInterval uint64
	if err := db.QueryRow(ctx, "SELECT value FROM system.settings WHERE name = 'log_queries_min_interval_ms'").Scan(&flushInterval); err == nil {
		cfg.QueryLog.FlushInterval = flushInterval
	}

	// 3. Check Table Size and Range (system.query_log)
	// This might be slow on big tables, use basic metadata if possible, or simple min/max
	// system.parts is better for size

	// Size
	var sizeBytes uint64
	db.QueryRow(ctx, "SELECT sum(bytes_on_disk) FROM system.parts WHERE table = 'query_log' AND active = 1").Scan(&sizeBytes)
	cfg.QueryLog.Size = sizeBytes

	// Time Range
	// Only do this if table exists
	// db.QueryRow(ctx, "SELECT min(event_time), max(event_time) FROM system.query_log").Scan(&cfg.QueryLog.Oldest, &cfg.QueryLog.Newest)
	// Optimize: Using system.parts min_time and max_time?
	// SELECT min(min_time), max(max_time) FROM system.parts WHERE table = 'query_log' AND active = 1
	var minT, maxT *time.Time
	if err := db.QueryRow(ctx, "SELECT min(min_time), max(max_time) FROM system.parts WHERE table = 'query_log' AND active = 1").Scan(&minT, &maxT); err == nil {
		if minT != nil {
			cfg.QueryLog.Oldest = *minT
		}
		if maxT != nil {
			cfg.QueryLog.Newest = *maxT
		}
	}

	return cfg, nil
}
