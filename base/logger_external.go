/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"

	"github.com/couchbase/clog"
	"github.com/couchbaselabs/rosmar"
)

// This file implements wrappers around the loggers of external packages
// so that all of SG's logging output is consistent.
func initExternalLoggers() {
	clog.SetLoggerCallback(ClogCallback)
	// Set the clog level to DEBUG and do filtering for debug inside ClogCallback functions
	clog.SetLevel(clog.LevelDebug)

	// Redirect Walrus logging to SG logs, and set an appropriate level:
	rosmar.LoggingCallback = rosmarLogger

	updateExternalLoggers()
}

func updateExternalLoggers() {
	// use context.Background() since this is called from init or to reset test logging
	logger := consoleLogger.Load()
	if logger.shouldLog(context.Background(), LevelDebug, KeyWalrus) {
		rosmar.SetLogLevel(rosmar.LevelDebug)
	} else {
		rosmar.SetLogLevel(rosmar.LevelInfo)
	}
}

// **************************************************************************
// Implementation of callback for github.com/couchbase/clog.SetLoggerCallback
//
//	Our main library that uses clog is cbgt, so all logging goes to KeyDCP.
//	Note that although sg-replicate uses clog's log levels, sgreplicateLogFn
//	bypasses clog logging, and so won't end up in this callback.
//
// **************************************************************************
func ClogCallback(level, format string, v ...any) string {
	switch level {
	case "ERRO", "FATA", "CRIT":
		logTo(context.TODO(), LevelError, KeyAll, KeyDCP.String()+": "+format, v...)
	case "WARN":
		// TODO: cbgt currently logs a lot of what we'd consider info as WARN,
		//    (i.e. diagnostic information that's not actionable by users), so
		//    routing to Info pending potential enhancements on cbgt side.
		logTo(context.TODO(), LevelInfo, KeyDCP, format, v...)
	case "INFO":
		// TODO: cbgt currently logs a lot of what we'd consider debug as INFO,
		//    (i.e. janitor work and partition status), so
		//    routing to Debug pending potential enhancements on cbgt side.
		logTo(context.TODO(), LevelDebug, KeyDCP, format, v...)
	case "DEBU":
		logTo(context.TODO(), LevelDebug, KeyDCP, format, v...)
	case "TRAC":
		logTo(context.TODO(), LevelTrace, KeyDCP, format, v...)
	}
	return ""
}

// **************************************************************************
// Log callback for Rosmar
// **************************************************************************
func rosmarLogger(level rosmar.LogLevel, fmt string, args ...any) {
	sgLevel := LogLevel(level)
	// info logging in Rosmar is extremely verbose (view queried, dcp message sent, etc.) - drop down to debug
	if level == rosmar.LevelInfo {
		sgLevel = LevelDebug
	}
	key := KeyWalrus
	if sgLevel <= LevelWarn {
		key = KeyAll
		fmt = "Rosmar: " + fmt
	}
	logTo(context.TODO(), sgLevel, key, fmt, args...)
}
