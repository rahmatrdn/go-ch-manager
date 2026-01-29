package handler

import (
	"encoding/json"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/rahmatrdn/go-ch-manager/internal/usecase"
)

type ReportHandler struct {
	reportUsecase     usecase.ReportUsecase
	connectionUsecase *usecase.ConnectionUsecase
}

func NewReportHandler(reportUsecase usecase.ReportUsecase, connectionUsecase *usecase.ConnectionUsecase) *ReportHandler {
	return &ReportHandler{
		reportUsecase:     reportUsecase,
		connectionUsecase: connectionUsecase,
	}
}

func (h *ReportHandler) Register(app *fiber.App) {
	group := app.Group("/connections/:id/reports")
	group.Get("/slow-queries", h.GetSlowQueries)
}

func (h *ReportHandler) GetSlowQueries(c *fiber.Ctx) error {
	connectionID, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid Connection ID")
	}

	refresh := c.Query("refresh") == "true"

	reports, lastRefresh, err := h.reportUsecase.GetTopSlowQueries(c.Context(), connectionID, refresh)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
	}

	if c.Query("format") == "json" || c.Get("Accept") == "application/json" {
		return c.JSON(fiber.Map{
			"data":         reports,
			"last_refresh": lastRefresh,
		})
	}

	// Marshaling reports to JSON for frontend
	reportsJSON, _ := json.Marshal(reports)
	if len(reportsJSON) == 0 {
		reportsJSON = []byte("[]")
	}

	// Fetch connections for sidebar
	connections, _ := h.connectionUsecase.GetAllConnections(c.Context())

	return c.Render("reports/index", fiber.Map{
		"Reports":            string(reportsJSON),
		"ConnectionID":       connectionID,
		"LastRefresh":        lastRefresh,
		"ActiveMenu":         " reports",
		"SidebarConnections": connections,
	}, "layouts/main")
}
