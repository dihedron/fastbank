package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
)

func init() {
	const LevelNone = slog.Level(1000)

	options := &slog.HandlerOptions{
		Level:     LevelNone,
		AddSource: true,
	}

	// my-app -> MY_APP_LOG_LEVEL
	level, ok := os.LookupEnv(
		fmt.Sprintf(
			"%s_LOG_LEVEL",
			strings.ReplaceAll(
				strings.ToUpper(
					path.Base(os.Args[0]),
				),
				"-",
				"_",
			),
		),
	)
	if ok {
		switch strings.ToLower(level) {
		case "debug", "dbg", "d", "trace", "trc", "t":
			options.Level = slog.LevelDebug
		case "informational", "info", "inf", "i":
			options.Level = slog.LevelInfo
		case "warning", "warn", "wrn", "w":
			options.Level = slog.LevelWarn
		case "error", "err", "e", "fatal", "ftl", "f":
			options.Level = slog.LevelError
		case "off", "none", "null", "nil", "no", "n":
			options.Level = LevelNone
			return
		}
	}

	// my-app -> MY_APP_LOG_STREAM
	var writer io.Writer = os.Stderr
	stream, ok := os.LookupEnv(
		fmt.Sprintf(
			"%s_LOG_STREAM",
			strings.ReplaceAll(
				strings.ToUpper(
					path.Base(os.Args[0]),
				),
				"-",
				"_",
			),
		),
	)
	if ok {
		switch strings.ToLower(stream) {
		case "stderr", "error", "err", "e":
			writer = os.Stderr
		case "stdout", "output", "out", "o":
			writer = os.Stdout
		default:
			// fmt.Printf("opening file: %s\n", stream)
			var err error
			writer, err = os.Create(stream)
			if err != nil {
				writer = os.Stderr
			}
		}
	}

	handler := slog.NewTextHandler(writer, options)
	slog.SetDefault(slog.New(handler))
}
