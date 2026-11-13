// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Command postier is the single binary of the platform. Each role is a
// subcommand; a deployment runs the same image with different arguments.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"

	"github.com/SeeMyPing/postier/internal/platform"
)

// version is set at build time (-ldflags "-X main.version=…").
var version = "dev"

type command struct {
	summary string
	run     func(ctx context.Context, args []string) error
}

var commands = map[string]command{
	"api":      {"serve the REST API", runAPI},
	"dispatch": {"inject accepted messages into KumoMTA", runDispatch},
	"ingest":   {"receive KumoMTA log records and update message statuses", runIngest},
	"migrate":  {"apply database migrations, partitions and grants", runMigrate},
	"seed":     {"create a tenant, a project, a sending domain and an API key (phase 0)", runSeed},
	"dkim":     {"generate a DKIM key and print its DNS record", runDKIM},
	"version":  {"print the version", runVersion},
}

func usage(w io.Writer) {
	// Writing the help to a closed stdout or stderr is not worth reporting.
	printf := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	printf("Postier %s: Kubernetes-native transactional email\n\nUsage:\n  postier <command> [flags]\n\nCommands:\n", version)
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		printf("  %-9s %s\n", name, commands[name].summary)
	}
	printf("\nRun 'postier <command> -h' for the flags of a command.\n" +
		"Every flag can also be set through the environment: --kumo-url is POSTIER_KUMO_URL.\n")
}

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	name := os.Args[1]
	if name == "-h" || name == "--help" || name == "help" {
		usage(os.Stdout)
		return
	}
	cmd, ok := commands[name]
	if !ok {
		fmt.Fprintf(os.Stderr, "postier: unknown command %q\n\n", name)
		usage(os.Stderr)
		os.Exit(2)
	}
	if err := cmd.run(context.Background(), os.Args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "postier %s: %v\n", name, err)
		os.Exit(1)
	}
}

func runVersion(context.Context, []string) error {
	fmt.Println(version)
	return nil
}

// common holds the settings every long-running role shares.
type common struct {
	logLevel  string
	logFormat string
	probeAddr string
}

func (c *common) register(f *platform.Flags) {
	f.String(&c.logLevel, "log-level", "info", "log level: debug, info, warn or error")
	f.String(&c.logFormat, "log-format", "json", "log format: json or text")
	f.String(&c.probeAddr, "probe-addr", ":8090", "listen address of /healthz and /readyz")
}

func (c *common) logger(role string) (*slog.Logger, error) {
	log, err := platform.NewLogger(os.Stdout, c.logLevel, c.logFormat, role)
	if err != nil {
		return nil, err
	}
	// River and other libraries log through the default logger.
	slog.SetDefault(log)
	return log, nil
}
