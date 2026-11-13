// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package platform is the plumbing shared by every role: configuration,
// logging, probes and graceful shutdown.
package platform

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// EnvPrefix prefixes every environment variable read by Postier.
const EnvPrefix = "POSTIER_"

// Flags declares settings that can come from a command-line flag or from the
// environment. The flag wins; the environment variable is the flag name in
// upper case with dashes turned into underscores, prefixed with POSTIER_
// (--database-url is POSTIER_DATABASE_URL). Pods are configured through the
// environment, people through flags.
type Flags struct {
	fs     *flag.FlagSet
	getenv func(string) string
	errs   []string
}

// NewFlags returns a flag set for the given subcommand.
func NewFlags(name string) *Flags {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	return &Flags{fs: fs, getenv: os.Getenv}
}

// EnvName returns the environment variable matching a flag name.
func EnvName(flagName string) string {
	return EnvPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

func (f *Flags) usage(name, usage string) string {
	return fmt.Sprintf("%s (env %s)", usage, EnvName(name))
}

// String declares a string setting.
func (f *Flags) String(p *string, name, def, usage string) {
	if v := f.getenv(EnvName(name)); v != "" {
		def = v
	}
	f.fs.StringVar(p, name, def, f.usage(name, usage))
}

// Secret declares a setting that must not show up in `ps` or in --help
// output: it is read from the environment only.
func (f *Flags) Secret(p *string, name string) {
	*p = f.getenv(EnvName(name))
}

// Int declares an integer setting.
func (f *Flags) Int(p *int, name string, def int, usage string) {
	if v := f.getenv(EnvName(name)); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			f.errs = append(f.errs, fmt.Sprintf("%s: %v", EnvName(name), err))
		} else {
			def = n
		}
	}
	f.fs.IntVar(p, name, def, f.usage(name, usage))
}

// Duration declares a duration setting ("30s", "5m").
func (f *Flags) Duration(p *time.Duration, name string, def time.Duration, usage string) {
	if v := f.getenv(EnvName(name)); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			f.errs = append(f.errs, fmt.Sprintf("%s: %v", EnvName(name), err))
		} else {
			def = d
		}
	}
	f.fs.DurationVar(p, name, def, f.usage(name, usage))
}

// Bool declares a boolean setting.
func (f *Flags) Bool(p *bool, name string, def bool, usage string) {
	if v := f.getenv(EnvName(name)); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			f.errs = append(f.errs, fmt.Sprintf("%s: %v", EnvName(name), err))
		} else {
			def = b
		}
	}
	f.fs.BoolVar(p, name, def, f.usage(name, usage))
}

// Parse parses the command line. It reports invalid environment values too.
func (f *Flags) Parse(args []string) error {
	if len(f.errs) > 0 {
		return fmt.Errorf("invalid environment: %s", strings.Join(f.errs, "; "))
	}
	return f.fs.Parse(args)
}

// Args returns the positional arguments left after Parse.
func (f *Flags) Args() []string { return f.fs.Args() }

// Require returns an error naming the settings that are still empty.
func Require(settings map[string]string) error {
	var missing []string
	for name, v := range settings {
		if v == "" {
			missing = append(missing, fmt.Sprintf("--%s / %s", name, EnvName(name)))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	// Stable output, whatever the map order.
	sortStrings(missing)
	return fmt.Errorf("missing required settings: %s", strings.Join(missing, ", "))
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
