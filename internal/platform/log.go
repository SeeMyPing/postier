// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/SeeMyPing/postier/internal/domain"
)

// NewLogger builds the structured logger. JSON is the default; "text" is for
// a terminal.
func NewLogger(w io.Writer, level, format, role string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch strings.ToLower(format) {
	case "", "json":
		h = slog.NewJSONHandler(w, opts)
	case "text":
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("log format %q: want json or text", format)
	}
	return slog.New(h).With("role", role), nil
}

// AddrAttr is the only way an email address may reach the logs: as a prefix
// of its hash. Logging the address itself would make an erasure request
// impossible to honour (docs/implementation-plan.md §2.14).
func AddrAttr(key string, a domain.Address) slog.Attr {
	h := a.Hash()
	return slog.String(key, hex.EncodeToString(h[:8]))
}
