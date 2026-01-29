package entity

import "time"

// ConfigurationData holds all data for the configuration page
type ConfigurationData struct {
	ClusterInfo     ClusterInfo
	Users           []CHUser
	Roles           []CHRole
	CreateStatement string // For current user/profile if needed?
	Settings        []CHSetting
	StoragePolicies []StoragePolicy
	Disks           []Disk
	Quotas          []Quota
	Processes       ProcessStats
	LogConfig       LogConfig
}

type ClusterInfo struct {
	ClusterName     string
	Environment     string // Derived or static
	Host            string
	Port            int
	Protocol        string
	DatabaseDefault string
	ReadWriteMode   bool // true = RW
	Shards          int
	Replicas        int
	Nodes           []string
	Version         string
	Uptime          int64
	Timezone        string
	DisplayName     string
}

type CHUser struct {
	Name            string
	ID              string
	Storage         string
	AuthType        string
	AuthParams      string
	HostIP          string
	Grantees        []string
	DefaultRoles    []string
	DefaultDatabase string
	Profile         string
	Quota           string
}

type CHRole struct {
	Name     string
	ID       string
	Storage  string
	Grantees []string
}

type CHSetting struct {
	Name        string
	Value       string
	Changed     bool
	Description string
	Type        string
	Readonly    int
}

type StoragePolicy struct {
	Name               string
	Volumes            []StorageVolume
	MoveFactor         float64
	KeepFreeSpaceBytes uint64
}

type StorageVolume struct {
	Name            string
	Disks           []string
	MaxDataPartSize uint64
}

type Disk struct {
	Name          string
	Path          string
	FreeSpace     uint64
	TotalSpace    uint64
	KeepFreeSpace uint64
	Type          string
}

type Quota struct {
	Name          string
	Key           string
	Duration      uint64
	Queries       uint64
	QuerySelects  uint64
	QueryInserts  uint64
	Errors        uint64
	ResultRows    uint64
	ResultBytes   uint64
	ReadRows      uint64
	ReadBytes     uint64
	ExecutionTime uint64
}

type ProcessStats struct {
	MemoryTracking    int64 // Total memory tracking
	GlobalMemoryLimit int64
	QueriesInProgress int
	BackgroundMerges  int
	BackgroundFetches int
}

type LogConfig struct {
	QueryLog struct {
		Enabled       bool
		FlushInterval uint64
		Size          uint64
		Oldest        time.Time
		Newest        time.Time
	}
	// Add others as needed
}
