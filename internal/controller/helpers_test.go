// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package controller

import (
	"io"
	"log/slog"

	"go.uber.org/zap/zapcore"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// zapErrorOnly keeps controller-runtime quiet in tests, except for errors.
type zapErrorOnly struct{}

func (zapErrorOnly) Enabled(l zapcore.Level) bool { return l >= zapcore.DPanicLevel }
