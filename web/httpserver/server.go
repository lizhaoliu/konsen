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
}

func (s *Server) getHandler(c *gin.Context) {
	key := c.Query("key")
	if key != "" {
		ctx := context.Background()
		buf, err := s.sm.GetValue(ctx, []byte(key))
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		c.String(http.StatusOK, string(buf))
	}
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
	ctx := context.Background()
	if err := s.sm.SetKeyValue(ctx, kvList); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "")
}

func (s *Server) Run() error {
	return s.httpServer.ListenAndServe()
}
