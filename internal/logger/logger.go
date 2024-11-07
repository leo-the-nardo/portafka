package logger

import (
	"go.uber.org/zap"
)

var Logger *zap.Logger

func InitializeLogger() {
	logger, _ = zap.NewProduction()
}
