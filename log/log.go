package log

import (
	"os"

	"github.com/TheZeroSlave/zapsentry"
	"github.com/getsentry/sentry-go"
	ltsv "github.com/hnakamur/zap-ltsv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/crypto/ssh/terminal"
)

type Logger struct {
	cfg                  *Config
	logger, sentryLogger *zap.Logger
}

type Config struct {
	Debug    bool
	LogLevel string
}

type Option func(*Config)

func WithDebug(debug bool) Option {
	return func(config *Config) {
		config.Debug = debug
	}
}

func WithLogLevel(logLevel string) Option {
	return func(config *Config) {
		config.LogLevel = logLevel
	}
}

// Init sets up a logger.
func New(sc *sentry.Client, opts ...Option) (*Logger, error) {
	var (
		config zap.Config
		cfg    Config
	)

	for _, opt := range opts {
		opt(&cfg)
	}

	if os.Getenv("FORCE_TERM") == "1" || terminal.IsTerminal(int(os.Stdout.Fd())) {
		config = zap.NewDevelopmentConfig()
		if cfg.Debug {
			config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		}
	} else {
		if err := ltsv.RegisterLTSVEncoder(); err != nil {
			return nil, err
		}
		config = ltsv.NewProductionConfig()
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	if cfg.Debug {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else if cfg.LogLevel != "" {
		var l zapcore.Level

		if err := l.Set(cfg.LogLevel); err != nil {
			return nil, err
		}

		config.Level.SetLevel(l)
	}

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	if sc == nil {
		return &Logger{
			logger: logger,
			cfg:    &cfg,
		}, err
	}

	sentryLogger, err := addSentryLogger(logger, sc)

	return &Logger{
		logger:       logger,
		sentryLogger: sentryLogger,
		cfg:          &cfg,
	}, err
}

func addSentryLogger(log *zap.Logger, sc *sentry.Client) (*zap.Logger, error) {
	cfg := zapsentry.Configuration{
		Level: zapcore.ErrorLevel,
	}
	core, err := zapsentry.NewCore(cfg, zapsentry.NewSentryClientFromClient(sc))

	return zapsentry.AttachCoreToLogger(core, log), err
}

// RawLogger returns zap logger without sentry.
func (l *Logger) RawLogger() *zap.Logger {
	return l.logger
}

// Logger returns zap logger with sentry support on error.
func (l *Logger) Logger() *zap.Logger {
	return l.sentryLogger
}

// Sync syncs both loggers.
func (l *Logger) Sync() {
	l.logger.Sync()       // nolint: errcheck
	l.sentryLogger.Sync() // nolint: errcheck
}
