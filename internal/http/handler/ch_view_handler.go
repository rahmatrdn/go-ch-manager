package handler

import (
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/usecase"
)

type ViewHandler struct {
	usecase *usecase.ConnectionUsecase
}

func NewViewHandler(usecase *usecase.ConnectionUsecase) *ViewHandler {
	return &ViewHandler{
		usecase: usecase,
	}
}

func (h *ViewHandler) Register(api fiber.Router) {
	// Web Routes
	api.Get("/", h.Dashboard)
	api.Get("/connections/create", h.CreateConnectionView)
	api.Post("/connections/create", h.HandleCreateConnection)
	api.Get("/connections/:id/edit", h.EditConnectionView)
	api.Post("/connections/:id/edit", h.HandleUpdateConnection)
	api.Get("/connections/:id", h.ConnectionMenu)          // Menu Page
	api.Get("/connections/:id/tables", h.ConnectionTables) // Stats & Table List
	api.Get("/connections/:id/tables", h.ConnectionTables) // Stats & Table List
	api.Get("/connections/:id/tables/:table", h.TableDetails)
	api.Get("/connections/:id/compare", h.ComparePage)
	api.Post("/connections/:id/compare/favorite", h.SaveCompareFavorite)
	api.Get("/connections/:id/compare/favorites", h.GetCompareFavorites)
	api.Delete("/connections/:id/compare/favorites/:fav_id", h.DeleteCompareFavorite)
	api.Get("/connections/:id/console", h.ConsolePage)
	api.Get("/connections/:id/configuration", h.ConfigurationPage)
}

// Helper to render view with global data (Sidebar)
func (h *ViewHandler) render(c *fiber.Ctx, view string, data fiber.Map, layout ...string) error {
	// Fetch all connections for Sidebar
	conns, err := h.usecase.GetAllConnections(c.Context())
	if err == nil {
		data["SidebarConnections"] = conns
	}

	lay := "layouts/main"
	if len(layout) > 0 {
		lay = layout[0]
	}

	return c.Render(view, data, lay)
}

func (h *ViewHandler) Dashboard(c *fiber.Ctx) error {
	conns, err := h.usecase.GetAllConnections(c.Context())
	if err != nil {
		return c.Status(500).SendString(err.Error())
	}

	return h.render(c, "index", fiber.Map{
		"Connections": conns,
		"PageTitle":   "Dashboard",
	})
}

func (h *ViewHandler) CreateConnectionView(c *fiber.Ctx) error {
	return h.render(c, "connections/create", fiber.Map{
		"PageTitle": "New Connection",
	})
}

func (h *ViewHandler) HandleCreateConnection(c *fiber.Ctx) error {
	var conn entity.CHConnection
	if err := c.BodyParser(&conn); err != nil { // Form parsing
		return c.Status(400).SendString("Invalid form data")
	}

	// Checkbox handling: HTML forms don't send anything for unchecked boxes
	conn.UseSSL = c.FormValue("use_ssl") == "on"

	if conn.Label == "" {
		conn.Label = "DEVELOPMENT"
	}

	if err := h.usecase.CreateConnection(c.Context(), &conn); err != nil {
		return h.render(c, "connections/create", fiber.Map{
			"PageTitle": "New Connection",
			"Error":     err.Error(),
			"Form":      conn,
		})
	}
	return c.Redirect("/")
}

func (h *ViewHandler) EditConnectionView(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	conns, err := h.usecase.GetAllConnections(c.Context())
	if err != nil {
		return c.Status(500).SendString(err.Error())
	}

	var target *entity.CHConnection
	for i := range conns {
		if conns[i].ID == id {
			target = conns[i]
			break
		}
	}

	if target == nil {
		return c.Status(404).SendString("Connection not found")
	}

	return h.render(c, "connections/edit", fiber.Map{
		"Connection": target,
		"PageTitle":  "Edit Connection",
	})
}

// Using POST for Edit form as HTML forms support GET/POST
func (h *ViewHandler) HandleUpdateConnection(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	var conn entity.CHConnection
	if err := c.BodyParser(&conn); err != nil {
		return c.Status(400).SendString("Invalid form data")
	}

	conn.ID = id
	conn.UseSSL = c.FormValue("use_ssl") == "on"

	if conn.Label == "" {
		conn.Label = "DEVELOPMENT"
	}

	if err := h.usecase.UpdateConnection(c.Context(), id, &conn); err != nil {
		return c.Status(500).SendString(err.Error())
	}
	return c.Redirect("/")
}

func (h *ViewHandler) ConnectionMenu(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	conns, err := h.usecase.GetAllConnections(c.Context()) // Might fetch twice if using render helper, but okay for now
	if err != nil {
		return c.Status(500).SendString(err.Error())
	}

	var target *entity.CHConnection
	for i := range conns {
		if conns[i].ID == id {
			target = conns[i]
			break
		}
	}

	if target == nil {
		return c.Status(404).SendString("Connection not found")
	}

	// Fetch Server Info
	serverInfo, _ := h.usecase.GetServerInfo(c.Context(), id)

	return h.render(c, "connections/menu", fiber.Map{
		"Connection":   target,
		"ConnectionID": target.ID,
		"ServerInfo":   serverInfo,
		"PageTitle":    "Connection: " + target.Name,
		"ActiveMenu":   " overview",
	})
}

func (h *ViewHandler) ConnectionTables(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	requestDB := c.Query("db")

	// Get available databases
	dbs, err := h.usecase.GetDatabases(c.Context(), id)
	if err != nil {
		return h.render(c, "error", fiber.Map{"Error": err.Error()})
	}

	// We no longer fetch tables here for SSR.
	// Just determine the initially selected database to pass to the view.
	selectedDB := requestDB

	// Need to know the default database if selectedDB is empty to show it in UI/Title
	// We can get it from the connection object, which we might want to pass anyway.
	var defaultDB string

	// Helper to find connection again (optimization: GetAllConnections is already called in render,
	// but we need it here for logic. Maybe render helper should expose it or we fetch it first).
	// For now, let's fetch specific connection to get its default DB.
	// In a real optimized app we'd cache or use the data from GetAllConnections if we refactored render.
	// But let's just cheat and assume if selectedDB is empty, frontend handles "default".

	// Actually, let's fetch the connection to pass its Name/DefaultDB to the view for the title.
	conns, _ := h.usecase.GetAllConnections(c.Context())
	var conn *entity.CHConnection
	for _, c := range conns {
		if c.ID == id {
			conn = c
			break
		}
	}

	if conn != nil && selectedDB == "" {
		defaultDB = conn.Database
		if defaultDB == "" {
			defaultDB = "default"
		}
		selectedDB = defaultDB
	} else if selectedDB == "" {
		selectedDB = "default" // Fallback
	}

	return h.render(c, "connections/show", fiber.Map{
		"ConnectionID": id,
		"PageTitle":    "Dashboard",
		"Databases":    dbs,
		"SelectedDB":   selectedDB,
		"ActiveMenu":   " explorer",
		// Removing GroupedTables, Stats, TotalTables as they are now JS driven
	})
}

func (h *ViewHandler) TableDetails(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	tableName := c.Params("table")
	dbName := c.Query("db")

	schema, createSQL, err := h.usecase.GetSchema(c.Context(), id, tableName, dbName)
	if err != nil {
		return h.render(c, "error", fiber.Map{"Error": err.Error()})
	}

	return h.render(c, "tables/show", fiber.Map{
		"ConnectionID": id,
		"Schema":       schema,
		"CreateSQL":    createSQL,
		"PageTitle":    "Table: " + tableName,
		"ActiveMenu":   " explorer",
	})
}

func (h *ViewHandler) ComparePage(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	conn, err := h.usecase.GetConnectionStatus(c.Context(), id)
	if err != nil {
		// handle error or redirect
	}

	return h.render(c, "connections/compare", fiber.Map{
		"ConnectionID": id,
		"Status":       conn,
		"ActiveMenu":   " compare",
	})
}

func (h *ViewHandler) SaveCompareFavorite(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	var input struct {
		Title  string `json:"title"`
		Query1 string `json:"query1"`
		Query2 string `json:"query2"`
	}

	if err := c.BodyParser(&input); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	fav := &entity.FavoriteComparison{
		ConnectionID: id,
		Title:        input.Title,
		Query1:       input.Query1,
		Query2:       input.Query2,
	}

	if err := h.usecase.SaveFavoriteComparison(c.Context(), fav); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"message": "Saved successfully", "data": fav})
}

func (h *ViewHandler) GetCompareFavorites(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	favs, err := h.usecase.GetFavoriteComparisons(c.Context(), id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(favs)
}

func (h *ViewHandler) DeleteCompareFavorite(c *fiber.Ctx) error {
	favID, _ := strconv.ParseInt(c.Params("fav_id"), 10, 64)
	if err := h.usecase.DeleteFavoriteComparison(c.Context(), favID); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"message": "Deleted successfully"})
}

func (h *ViewHandler) ConsolePage(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)

	return h.render(c, "connections/console", fiber.Map{
		"ConnectionID": id,
		"PageTitle":    "Query Console",
		"ActiveMenu":   " console",
	})
}

func (h *ViewHandler) ConfigurationPage(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)

	// Check connection status first? Or just fetch config.
	// We want to fetch all config data
	data, err := h.usecase.GetConfigurationData(c.Context(), id)
	if err != nil {
		return h.render(c, "error", fiber.Map{"Error": err.Error()})
	}
	if data == nil {
		return c.Status(404).SendString("Connection not found")
	}

	return h.render(c, "connections/configuration", fiber.Map{
		"ConnectionID": id,
		"Data":         data,
		"PageTitle":    "Configuration",
		"ActiveMenu":   " configuration",
	})
}
