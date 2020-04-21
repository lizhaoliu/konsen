package main

import (
	"context"
	"os"

	"github.com/lizhaoliu/konsen/v2/core"
	"github.com/sirupsen/logrus"
)

func init() {
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.TraceLevel)
}

func main() {
	logrus.Infoln("Hello, world!")
	sm := &core.StateMachine{}
	sm.Run(context.Background())
}
