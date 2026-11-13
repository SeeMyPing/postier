// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Command capture turns batches recorded by `postier ingest --capture-dir`
// into the reference files of internal/kumo/testdata.
//
// The decoder and the status mapping are tested against real records, never
// against records written from the documentation: the two differ. The files
// are regenerated with `make capture` every time the pinned KumoMTA version
// changes, and the diff is the review of what upstream changed.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
)

func main() {
	in := flag.String("in", "", "directory of captured batch-*.json files")
	out := flag.String("out", "internal/kumo/testdata", "directory to write the reference files to")
	flag.Parse()
	if *in == "" {
		log.Fatal("capture: -in is required")
	}

	files, err := filepath.Glob(filepath.Join(*in, "batch-*.json"))
	if err != nil || len(files) == 0 {
		log.Fatalf("capture: no batch found in %s", *in)
	}
	sort.Strings(files) // file names carry the reception time

	var session []json.RawMessage
	first := map[string]json.RawMessage{}
	sizes := map[int]int{}
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			log.Fatal(err)
		}
		var batch []json.RawMessage
		if err := json.Unmarshal(body, &batch); err != nil {
			log.Fatalf("capture: %s: %v", file, err)
		}
		sizes[len(batch)]++
		for _, raw := range batch {
			var head struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(raw, &head); err != nil {
				log.Fatalf("capture: %s: %v", file, err)
			}
			session = append(session, raw)
			if _, seen := first[head.Type]; !seen {
				first[head.Type] = raw
			}
		}
	}

	records := filepath.Join(*out, "records")
	if err := os.MkdirAll(records, 0o755); err != nil {
		log.Fatal(err)
	}
	for typ, raw := range first {
		write(filepath.Join(records, typ+".json"), raw)
	}
	all, err := json.Marshal(session)
	if err != nil {
		log.Fatal(err)
	}
	write(filepath.Join(*out, "session.json"), all)

	fmt.Printf("%d records in %d batches, %d types\n", len(session), len(files), len(first))
	fmt.Printf("batch sizes (size: count): %v\n", sizes)
}

func write(path string, raw []byte) {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		log.Fatal(err)
	}
	buf.WriteByte('\n')
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil { //nolint:gosec // test data
		log.Fatal(err)
	}
}
