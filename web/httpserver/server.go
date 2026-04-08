package httpserver

import (
	"context"
	_ "embed"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"github.com/sirupsen/logrus"
)

//go:embed static/index.html
var indexHTML []byte

const (
	serviceRelPath     = "/konsen"
	maxRequestBodySize = 1 << 20 // 1 MB
	maxListKeysLimit   = 1000
)

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
	s.router.Use(maxBodySizeMiddleware(maxRequestBodySize))

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
		logrus.Errorf("health check failed: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy", "error": "service unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "healthy", "role": role.String(), "leader": leader})
}

func (s *Server) readyHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	role, leader, err := s.sm.HealthCheck(ctx)
	if err != nil {
		logrus.Errorf("ready check failed: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "error": "service unavailable"})
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
		internalErrorString(c, err)
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
		internalErrorString(c, err)
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
		internalError(c, err)
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
		internalError(c, err)
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
	if limit > maxListKeysLimit {
		limit = maxListKeysLimit
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	var prefixBytes []byte
	if prefix != "" {
		prefixBytes = []byte(prefix)
	}
	keys, err := s.sm.ListKeys(ctx, prefixBytes, limit)
	if err != nil {
		internalError(c, err)
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

	status, err := s.sm.GetStatus(ctx)
	if err != nil {
		internalError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"node":          s.localServerName,
		"role":          status.Role.String(),
		"currentLeader": status.CurrentLeader,
		"currentTerm":   status.CurrentTerm,
		"commitIndex":   status.CommitIndex,
		"lastApplied":   status.LastApplied,
		"nextIndex":     status.NextIndex,
		"matchIndex":    status.MatchIndex,
		"logCount":      status.LogCount,
	})
}

func (s *Server) Run() error {
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// RunListener starts the HTTP server on an existing listener.
// This is useful for tests that need to know the actual bound address (e.g., when using ":0").
func (s *Server) RunListener(lis net.Listener) error {
	if err := s.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the server, allowing in-flight requests to complete.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func maxBodySizeMiddleware(maxSize int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxSize)
		}
		c.Next()
	}
}

func internalError(c *gin.Context, err error) {
	logrus.Errorf("internal error: %v", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
}

func internalErrorString(c *gin.Context, err error) {
	logrus.Errorf("internal error: %v", err)
	c.String(http.StatusInternalServerError, "internal server error")
}
