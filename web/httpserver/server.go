package httpserver

import (
	"context"
	_ "embed"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
)

//go:embed static/index.html
var indexHTML []byte

const serviceRelPath = "/konsen"

type Server struct {
	sm              *core.StateMachine
	localServerName string
	router          *gin.Engine
	httpServer      *http.Server
}

type ServerConfig struct {
	StateMachine    *core.StateMachine
	Address         string
	LocalServerName string
}

func NewServer(config ServerConfig) *Server {
	router := gin.Default()

	httpServer := &http.Server{
		Addr:         config.Address,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	httpServer.SetKeepAlivesEnabled(false)

	s := &Server{
		sm:              config.StateMachine,
		localServerName: config.LocalServerName,
		router:          router,
		httpServer:      httpServer,
	}

	s.initialize()

	return s
}

func (s *Server) initialize() {
	// UI
	s.router.GET("/", s.indexHandler)

	// Existing endpoints
	s.router.GET(serviceRelPath, s.getHandler)
	s.router.POST(serviceRelPath, s.postHandler)
	s.router.GET("/health", s.healthHandler)
	s.router.GET("/ready", s.readyHandler)

	// JSON API
	api := s.router.Group("/api")
	api.GET("/kv", s.apiGetHandler)
	api.POST("/kv", s.apiPutHandler)
	api.GET("/keys", s.apiListKeysHandler)
	api.GET("/status", s.apiStatusHandler)
}

// indexHandler serves the embedded HTML UI.
func (s *Server) indexHandler(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", indexHTML)
}

func (s *Server) healthHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	role, leader, err := s.sm.HealthCheck(ctx)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy", "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "healthy", "role": role.String(), "leader": leader})
}

func (s *Server) readyHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	role, leader, err := s.sm.HealthCheck(ctx)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "error": err.Error()})
		return
	}
	// Node is ready if it is the leader, or if it knows who the leader is.
	if role == konsen.Role_LEADER || leader != "" {
		c.JSON(http.StatusOK, gin.H{"status": "ready", "role": role.String(), "leader": leader})
		return
	}
	c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "reason": "no leader elected"})
}

func (s *Server) getHandler(c *gin.Context) {
	key := c.Query("key")
	if key == "" {
		c.String(http.StatusBadRequest, "missing required query parameter: key")
		return
	}
	buf, err := s.sm.GetValue(c.Request.Context(), []byte(key))
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, string(buf))
}

func (s *Server) postHandler(c *gin.Context) {
	if err := c.Request.ParseForm(); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	var kvs []*konsen.KV
	for key, value := range c.Request.PostForm {
		if len(value) > 0 {
			kvs = append(kvs, &konsen.KV{
				Key:   []byte(key),
				Value: []byte(value[0]),
			})
		}
	}
	kvList := &konsen.KVList{KvList: kvs}
	if err := s.sm.SetKeyValue(c.Request.Context(), kvList); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "")
}

// --- JSON API handlers ---

func (s *Server) apiGetHandler(c *gin.Context) {
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing required query parameter: key"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	buf, err := s.sm.GetValue(ctx, []byte(key))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if buf == nil {
		c.JSON(http.StatusOK, gin.H{"key": key, "found": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "value": string(buf), "found": true})
}

func (s *Server) apiPutHandler(c *gin.Context) {
	var body struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if body.Key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	kvList := &konsen.KVList{
		KvList: []*konsen.KV{{Key: []byte(body.Key), Value: []byte(body.Value)}},
	}
	if err := s.sm.SetKeyValue(ctx, kvList); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (s *Server) apiListKeysHandler(c *gin.Context) {
	prefix := c.Query("prefix")
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 0 {
		limit = 100
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	var prefixBytes []byte
	if prefix != "" {
		prefixBytes = []byte(prefix)
	}
	keys, err := s.sm.ListKeys(ctx, prefixBytes, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	keyStrings := make([]string, len(keys))
	for i, k := range keys {
		keyStrings[i] = string(k)
	}
	c.JSON(http.StatusOK, gin.H{"keys": keyStrings})
}

func (s *Server) apiStatusHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	snapshot, err := s.sm.GetSnapshot(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"node":          s.localServerName,
		"role":          snapshot.Role.String(),
		"currentLeader": snapshot.CurrentLeader,
		"currentTerm":   snapshot.CurrentTerm,
		"commitIndex":   snapshot.CommitIndex,
		"lastApplied":   snapshot.LastApplied,
		"nextIndex":     snapshot.NextIndex,
		"matchIndex":    snapshot.MatchIndex,
		"logCount":      len(snapshot.LogIndices),
	})
}

func (s *Server) Run() error {
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the server, allowing in-flight requests to complete.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
