package main

import (
	"embed"
	"log"
	"os"
	"path/filepath"

	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/repository/clickhouse"
	"github.com/rahmatrdn/go-ch-manager/internal/repository/sqlite"
	"github.com/rahmatrdn/go-ch-manager/internal/usecase"
	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	"github.com/wailsapp/wails/v2/pkg/options/mac"
	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

//go:embed all:frontend/dist
var assets embed.FS

func main() {
	// SQLite Initialization
	baseDir, err := os.UserConfigDir()
	if err != nil {
		log.Fatal("Failed to resolve user config dir:", err)
	}
	dataDir := filepath.Join(baseDir, "go-ch-manager")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		log.Fatal("Failed to create SQLite directory:", err)
	}
	dbPath := filepath.Join(dataDir, "ch_manager.db")
	sqliteDB, err := gorm.Open(gormsqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to SQLite:", err)
	}
	if err := sqliteDB.AutoMigrate(&entity.CHConnection{}, &entity.SlowQueryReport{}, &entity.QueryHistory{}); err != nil {
		log.Fatal("Failed to migrate SQLite schema:", err)
	}

	// Initialize dependencies
	chClient := clickhouse.NewClickHouseClient()
	connectionRepo := sqlite.NewConnectionRepository(sqliteDB)
	historyRepo := sqlite.NewQueryHistoryRepository(sqliteDB)
	reportRepo := sqlite.NewReportRepository(sqliteDB)
	connectionUsecase := usecase.NewConnectionUsecase(connectionRepo, historyRepo, chClient)
	reportUsecase := usecase.NewReportUsecase(reportRepo, connectionRepo, chClient)

	// Create Wails app instance
	app := NewApp(connectionUsecase, reportUsecase)

	// Create application with options
	err = wails.Run(&options.App{
		Title:  "Go CH Manager",
		Width:  1280,
		Height: 800,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		BackgroundColour: &options.RGBA{R: 27, G: 38, B: 54, A: 1},
		OnStartup:        app.startup,
		Bind: []interface{}{
			app,
		},
		Mac: &mac.Options{
			TitleBar: &mac.TitleBar{
				TitlebarAppearsTransparent: true,
				HideTitle:                  false,
				HideTitleBar:               false,
				FullSizeContent:            true,
				UseToolbar:                 false,
			},
			Appearance: mac.NSAppearanceNameDarkAqua,
			About: &mac.AboutInfo{
				Title:   "Go CH Manager",
				Message: "ClickHouse Database Management Tool\n\nVersion 1.0.0",
			},
		},
	})

	if err != nil {
		log.Fatal("Error:", err)
	}
}
