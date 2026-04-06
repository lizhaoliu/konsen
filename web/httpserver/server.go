package httpserver

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lizhaoliu/konsen/v2/core"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
)

const serviceRelPath = "/konsen"

type Server struct {
	sm         *core.StateMachine
	router     *gin.Engine
	httpServer *http.Server
}

type ServerConfig struct {
	StateMachine *core.StateMachine
	Address      string
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
		sm:         config.StateMachine,
		router:     router,
		httpServer: httpServer,
	}

	s.initialize()

	return s
}

func (s *Server) initialize() {
	s.router.GET(serviceRelPath, s.getHandler)
	s.router.POST(serviceRelPath, s.postHandler)
	s.router.GET("/health", s.healthHandler)
	s.router.GET("/ready", s.readyHandler)
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
