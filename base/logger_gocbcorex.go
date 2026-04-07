/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// GocbcorexLogger returns a *zap.Logger that forwards all gocbcorex log output
// into the Sync Gateway logging system under the KeyGoCB log key.
//
// Log level mapping (gocbcorex zap → SG):
//
//	Error  → SG Error
//	Warn   → SG Warn
//	Info   → SG Debug  (remapped: gocbcorex is verbose at Info)
//	Debug  → SG Trace
func GocbcorexLogger() *zap.Logger {
	core := &sgZapCore{}
	return zap.New(core)
}

// sgZapCore is a zapcore.Core implementation that bridges zap structured
// logging from gocbcorex into Sync Gateway's logTo() function.
type sgZapCore struct{}

var _ zapcore.Core = (*sgZapCore)(nil)

// Enabled reports whether the given log level is enabled. We always return true
// and let SG's own logTo decide whether to actually emit the line, so that SG's
// runtime log-level configuration is respected.
func (c *sgZapCore) Enabled(zapcore.Level) bool {
	return true
}

// With returns a new Core with additional fields — since SG logging is
// format-string based, we do not propagate structured fields and just return
// the same core.
func (c *sgZapCore) With([]zapcore.Field) zapcore.Core {
	return c
}

// Check determines whether the entry should be logged. We add self as the core
// if the level is enabled.
func (c *sgZapCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return ce.AddCore(entry, c)
	}
	return ce
}

// Write converts a zap log entry + fields into a single SG log line.
func (c *sgZapCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	sgLevel, logKey := mapZapLevelToSG(entry.Level)

	// Build the message: start with the logger name (component) and message
	msg := entry.Message
	if entry.LoggerName != "" {
		msg = entry.LoggerName + ": " + msg
	}

	// Append structured fields as key=value pairs
	if len(fields) > 0 {
		enc := zapcore.NewMapObjectEncoder()
		for _, f := range fields {
			f.AddTo(enc)
		}
		for k, v := range enc.Fields {
			msg += " " + k + "=" + formatFieldValue(v)
		}
	}

	logTo(context.TODO(), sgLevel, logKey, "%s", msg)
	return nil
}

// Sync is a no-op; SG manages its own log flushing.
func (c *sgZapCore) Sync() error {
	return nil
}

// mapZapLevelToSG maps zap log levels to SG log levels and keys.
// The mapping follows the same remapping pattern used for gocb/gocbcore:
// gocbcorex Info is verbose, so it maps to SG Debug.
func mapZapLevelToSG(level zapcore.Level) (LogLevel, LogKey) {
	switch {
	case level >= zapcore.ErrorLevel:
		return LevelError, KeyAll
	case level >= zapcore.WarnLevel:
		return LevelWarn, KeyAll
	case level >= zapcore.InfoLevel:
		// gocbcorex Info is quite verbose (connection details, bootstrap, etc.)
		// Remap to SG Debug like we did for gocb
		return LevelDebug, KeyGoCB
	default:
		// Debug and below → SG Trace
		return LevelTrace, KeyGoCB
	}
}

// formatFieldValue converts a field value to a string representation.
func formatFieldValue(v any) string {
	if v == nil {
		return "<nil>"
	}
	if s, ok := v.(string); ok {
		return s
	}
	if e, ok := v.(error); ok {
		return e.Error()
	}
	return fmt.Sprintf("%v", v)
}
