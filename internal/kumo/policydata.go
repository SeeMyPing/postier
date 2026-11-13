// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package kumo

import (
	"encoding/json"
	"fmt"
	"sort"
)

// PolicyData is the data file read by kumo/policy/init.lua. The policy is
// fixed; this file is the only thing that changes when configuration does.
//
// In phase 0 it is written by hand (compose) or rendered by the Helm chart
// from values. From v0.1 the operator generates it from the CRDs, with this
// type as the single definition of the format.
type PolicyData struct {
	Version  int    `json:"version"`
	Hostname string `json:"hostname"`

	Listen  *Listen  `json:"listen,omitempty"`
	LogHook LogHook  `json:"log_hook"`
	Queue   *Timings `json:"queue,omitempty"`

	// Domains is keyed by sending domain (the domain of the From header).
	Domains map[string]DomainData `json:"domains"`
	// InboundDomains is the set of domains the inbound listener accepts mail
	// for: every return-path domain, plus the feedback loop domain. It is a
	// set, encoded as an object, because the policy looks domains up by key.
	InboundDomains map[string]bool `json:"inbound_domains"`
	// Routes overrides MX resolution, by destination domain ("*" matches
	// all). Empty in production.
	Routes map[string]Route `json:"routes,omitempty"`
}

// Listen holds the listener addresses. Changing them needs a restart.
type Listen struct {
	HTTP string `json:"http,omitempty"`
	SMTP string `json:"smtp,omitempty"`
}

// LogHook configures the delivery of log records to `postier ingest`.
type LogHook struct {
	URL              string `json:"url"`
	BatchSize        int    `json:"batch_size,omitempty"`
	MinBatchSize     int    `json:"min_batch_size,omitempty"`
	MaxBatchLatency  string `json:"max_batch_latency,omitempty"`
	Connections      int    `json:"connections,omitempty"`
	RetryInterval    string `json:"retry_interval,omitempty"`
	MaxRetryInterval string `json:"max_retry_interval,omitempty"`
	MaxAge           string `json:"max_age,omitempty"`
}

// Timings are the retry settings of the delivery queues.
type Timings struct {
	RetryInterval    string `json:"retry_interval,omitempty"`
	MaxRetryInterval string `json:"max_retry_interval,omitempty"`
	MaxAge           string `json:"max_age,omitempty"`
}

// DomainData is what the policy needs to sign for a sending domain.
type DomainData struct {
	Selector string `json:"selector"`
	KeyFile  string `json:"key_file"`
}

// Route sends a destination domain to a fixed host instead of its MX.
type Route struct {
	Host        string `json:"host"`
	Port        int    `json:"port,omitempty"`
	Connections int    `json:"connections,omitempty"`
}

// PolicyDataVersion is the format version this code writes.
const PolicyDataVersion = 1

// Validate checks what the policy assumes without checking.
func (d *PolicyData) Validate() error {
	if d.Version != PolicyDataVersion {
		return fmt.Errorf("policy data: version %d, want %d", d.Version, PolicyDataVersion)
	}
	if d.Hostname == "" {
		return fmt.Errorf("policy data: hostname is required")
	}
	if d.LogHook.URL == "" {
		return fmt.Errorf("policy data: log_hook.url is required")
	}
	if d.LogHook.BatchSize < 0 {
		return fmt.Errorf("policy data: log_hook.batch_size must not be negative")
	}
	names := make([]string, 0, len(d.Domains))
	for name := range d.Domains {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		dom := d.Domains[name]
		if dom.Selector == "" || dom.KeyFile == "" {
			return fmt.Errorf("policy data: domain %s needs a selector and a key_file", name)
		}
	}
	for name, r := range d.Routes {
		if r.Host == "" {
			return fmt.Errorf("policy data: route %s has no host", name)
		}
	}
	return nil
}

// Render validates and encodes the data file. Maps are encoded with sorted
// keys, so the output is stable: an unchanged configuration produces an
// unchanged file, and KumoMTA is not made to reload for nothing.
func (d *PolicyData) Render() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if d.Domains == nil {
		d.Domains = map[string]DomainData{}
	}
	if d.InboundDomains == nil {
		d.InboundDomains = map[string]bool{}
	}
	out, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}
