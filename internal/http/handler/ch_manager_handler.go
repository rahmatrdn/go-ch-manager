package handler

import (
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/rahmatrdn/go-ch-manager/entity"
	"github.com/rahmatrdn/go-ch-manager/internal/parser"
	"github.com/rahmatrdn/go-ch-manager/internal/presenter/json"
	"github.com/rahmatrdn/go-ch-manager/internal/usecase"
)

type ConnectionHandler struct {
	parser    parser.Parser
	presenter json.JsonPresenter
	usecase   *usecase.ConnectionUsecase
}

func NewConnectionHandler(parser parser.Parser, presenter json.JsonPresenter, usecase *usecase.ConnectionUsecase) *ConnectionHandler {
	return &ConnectionHandler{
		parser:    parser,
		presenter: presenter,
		usecase:   usecase,
	}
}

func (h *ConnectionHandler) Register(api fiber.Router) {
	connections := api.Group("/connections")
	connections.Post("", h.CreateConnection)
	connections.Put("/:id", h.UpdateConnection)
	connections.Get("", h.GetConnections)
	connections.Get("/:id/status", h.GetConnectionStatus)
	connections.Get("/:id/tables", h.GetConnectionTables)
	connections.Get("/:id/tables/:table/schema", h.GetTableSchema)
	connections.Post("/:id/compare-query", h.CompareQueries)
	connections.Post("/:id/compare-query", h.CompareQueries)
	connections.Get("/:id/history", h.GetConnectionHistory)
	connections.Post("/:id/query", h.HandleExecuteQuery)
}

func (h *ConnectionHandler) CreateConnection(c *fiber.Ctx) error {
	var conn entity.CHConnection
	// Using fiber's body parser directly or h.parser if it has specific logic.
	// Looking at existing code, h.parser usually validates.
	// Let's assume standard fiber BodyParser for now or check usage in other handlers.
	// existing: request, err := h.parser.ParseBodyRequest(c, payload)

	if err := c.BodyParser(&conn); err != nil {
		return h.presenter.BuildError(c, err)
	}

	err := h.usecase.CreateConnection(c.Context(), &conn)
	if err != nil {
		return h.presenter.BuildError(c, err)
	}

	return h.presenter.BuildSuccess(c, conn, "Connection Created", 201)
}

func (h *ConnectionHandler) UpdateConnection(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	var conn entity.CHConnection

	if err := c.BodyParser(&conn); err != nil {
		return h.presenter.BuildError(c, err)
	}

	// Ensure ID is set from params, safety check
	conn.ID = id

	err := h.usecase.UpdateConnection(c.Context(), id, &conn)
	if err != nil {
		return h.presenter.BuildError(c, err)
	}

	return h.presenter.BuildSuccess(c, conn, "Connection Updated", 200)
}

func (h *ConnectionHandler) GetConnections(c *fiber.Ctx) error {
	conns, err := h.usecase.GetAllConnections(c.Context())
	if err != nil {
		return h.presenter.BuildError(c, err)
	}
	return h.presenter.BuildSuccess(c, conns, "Connections Retrieved", 200)
}

func (h *ConnectionHandler) GetConnectionStatus(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	status, err := h.usecase.GetConnectionStatus(c.Context(), id)
	if err != nil {
		return h.presenter.BuildError(c, err)
	}
	return h.presenter.BuildSuccess(c, map[string]string{"status": status}, "Status Retrieved", 200)
}

func (h *ConnectionHandler) GetConnectionTables(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	db := c.Query("db")
	tables, err := h.usecase.GetTables(c.Context(), id, db)
	if err != nil {
		return h.presenter.BuildError(c, err)
	}
	return h.presenter.BuildSuccess(c, tables, "Tables Retrieved", 200)
}

func (h *ConnectionHandler) GetTableSchema(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	table := c.Params("table")
	schema, _, err := h.usecase.GetSchema(c.Context(), id, table)
	if err != nil {
		return h.presenter.BuildError(c, err)
	}
	return h.presenter.BuildSuccess(c, schema, "Schema Retrieved", 200)
}

type CompareRequest struct {
	Query1 string `json:"query1"`
	Query2 string `json:"query2"`
}

func (h *ConnectionHandler) CompareQueries(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	var req CompareRequest
	if err := c.BodyParser(&req); err != nil {
		return h.presenter.BuildError(c, err)
	}

	result, err := h.usecase.CompareQueries(c.Context(), id, req.Query1, req.Query2)
	if err != nil {
		return h.presenter.BuildError(c, err)
	}

	return h.presenter.BuildSuccess(c, result, "Comparison Completed", 200)
}

type ExecuteQueryRequest struct {
	Query string `json:"query"`
}

func (h *ConnectionHandler) HandleExecuteQuery(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	var req ExecuteQueryRequest
	if err := c.BodyParser(&req); err != nil {
		return h.presenter.BuildError(c, err)
	}

	result, err := h.usecase.ExecuteQuery(c.Context(), id, req.Query)
	if err != nil {
		return h.presenter.BuildError(c, err)
	}

	return h.presenter.BuildSuccess(c, result, "Query Executed", 200)
}

func (h *ConnectionHandler) GetConnectionHistory(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	history, err := h.usecase.GetQueryHistory(c.Context(), id)
	if err != nil {
		return h.presenter.BuildError(c, err)
	}
	return h.presenter.BuildSuccess(c, history, "History Retrieved", 200)
}
