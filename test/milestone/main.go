// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Command milestone is the acceptance test of phase 0, second half:
//
//	"Over 10 000 messages to the sink with a mix of responses, statuses are
//	100 % consistent, with no lost and no duplicated event, even when ingest
//	and then dispatch are killed during the send."
//
// It talks to a running test bench through its public API only. It sends
// messages whose outcome is decided by their recipient (see test/smtpsink),
// waits for each to settle, then checks every message: its final status, its
// whole event history, and that the sink never received it twice.
//
// It does not kill anything itself: `make milestone` does, while it runs.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// kind is a class of recipient and what must happen to its message.
type kind struct {
	name    string
	weight  int            // share of the traffic, in percent
	local   string         // recipient local part prefix
	status  string         // expected final status
	history *regexp.Regexp // expected event types, comma-joined, in order
	reaches bool           // whether the sink accepts the message
}

var kinds = []kind{
	{"delivered", 70, "ok", "delivered", regexp.MustCompile(`^accepted,queued,delivered$`), true},
	{"hard bounce at RCPT", 9, "550", "bounced", regexp.MustCompile(`^accepted,queued,bounced$`), false},
	{"hard bounce after DATA", 4, "data-554", "bounced", regexp.MustCompile(`^accepted,queued,bounced$`), false},
	{"deferred once, then delivered", 8, "defer1", "delivered", regexp.MustCompile(`^accepted,queued,deferred,delivered$`), true},
	{"asynchronous bounce", 5, "oob", "bounced", regexp.MustCompile(`^accepted,queued,delivered,bounced$`), true},
	{"complaint", 3, "fbl", "complained", regexp.MustCompile(`^accepted,queued,delivered,complained$`), true},
	{"deferred until expiry", 1, "421", "expired", regexp.MustCompile(`^accepted,queued(,deferred)+,expired$`), false},
}

type message struct {
	id   string
	kind *kind
	rcpt string
}

type client struct {
	api  string
	key  string
	http *http.Client
}

func (c *client) do(ctx context.Context, method, path string, body any, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.api+path, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: %w", method, path, err)
		}
	}
	return resp.StatusCode, nil
}

// send submits one message and retries until it is accepted. The idempotency
// key makes the retry safe: if the first try was accepted but its answer was
// lost, the second returns the same message instead of creating another.
func (c *client) send(ctx context.Context, run string, i int, k *kind) (message, error) {
	rcpt := fmt.Sprintf("%s-%s-%d@sink.test", k.local, run, i)
	body := map[string]any{
		"from":     "Acme <no-reply@acme.test>",
		"to":       []string{rcpt},
		"subject":  fmt.Sprintf("Milestone %s #%d", run, i),
		"html":     "<p>Bonjour, ceci est le message de test <b>#" + fmt.Sprint(i) + "</b>.</p>",
		"text":     "Bonjour, ceci est le message de test #" + fmt.Sprint(i) + ".",
		"tags":     []string{"milestone"},
		"metadata": map[string]string{"run": run},
	}
	var out struct {
		Messages []struct {
			ID string `json:"id"`
		} `json:"messages"`
	}
	var lastErr error
	for attempt := range 30 {
		req, _ := json.Marshal(body)
		hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.api+"/v1/emails", bytes.NewReader(req))
		if err != nil {
			return message{}, err
		}
		hreq.Header.Set("Authorization", "Bearer "+c.key)
		hreq.Header.Set("Content-Type", "application/json")
		hreq.Header.Set("Idempotency-Key", fmt.Sprintf("milestone-%s-%d", run, i))
		resp, err := c.http.Do(hreq)
		if err == nil {
			raw, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusAccepted {
				if err := json.Unmarshal(raw, &out); err != nil || len(out.Messages) != 1 {
					return message{}, fmt.Errorf("unexpected 202 body: %s", raw)
				}
				return message{id: out.Messages[0].ID, kind: k, rcpt: rcpt}, nil
			}
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw)
			if resp.StatusCode < 500 {
				return message{}, lastErr
			}
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return message{}, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	return message{}, lastErr
}

type email struct {
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
}

type eventList struct {
	Data []struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	} `json:"data"`
}

// check verifies one message. It returns "" when everything is as expected,
// "pending" when the message has simply not settled yet, or a description of
// what is wrong.
func (c *client) check(ctx context.Context, m message) string {
	var e email
	if code, err := c.do(ctx, http.MethodGet, "/v1/emails/"+m.id, nil, &e); err != nil || code != http.StatusOK {
		return fmt.Sprintf("GET failed: %d %v", code, err)
	}
	if e.Status != m.kind.status {
		// Not there yet, or stuck: the caller decides when the deadline hits.
		return "pending (status " + e.Status + ")"
	}
	var evs eventList
	if code, err := c.do(ctx, http.MethodGet, "/v1/emails/"+m.id+"/events", nil, &evs); err != nil || code != http.StatusOK {
		return fmt.Sprintf("GET events failed: %d %v", code, err)
	}
	types := make([]string, len(evs.Data))
	ids := map[string]bool{}
	for i, ev := range evs.Data {
		types[i] = ev.Type
		if ids[ev.ID] {
			return "event id listed twice"
		}
		ids[ev.ID] = true
	}
	history := strings.Join(types, ",")
	if !m.kind.history.MatchString(history) {
		// The status is right but the history is still filling in (the
		// queued event of a delivered message can arrive last).
		if len(history) < 200 && plausiblePrefix(m.kind, types) {
			return "pending (history " + history + ")"
		}
		return "wrong history: " + history
	}
	return ""
}

// plausiblePrefix reports whether the events seen so far could still grow
// into the expected history: no event type appears more often than expected.
func plausiblePrefix(k *kind, types []string) bool {
	count := map[string]int{}
	for _, t := range types {
		count[t]++
	}
	for t, n := range count {
		if t == "deferred" && strings.Contains(k.history.String(), "deferred") {
			continue
		}
		if n > 1 {
			return false // a duplicated event: never plausible
		}
		if !strings.Contains(k.history.String(), t) {
			return false // an event this kind of message never has
		}
	}
	return true
}

func pick(i int) *kind {
	n := i % 100
	for k := range kinds {
		if n < kinds[k].weight {
			return &kinds[k]
		}
		n -= kinds[k].weight
	}
	return &kinds[0]
}

func main() { os.Exit(run()) }

// run returns the process exit code: 0 when every message is exact.
func run() int {
	var (
		api         = flag.String("api", "http://localhost:8080", "base URL of the Postier API")
		sink        = flag.String("sink", "http://localhost:8026", "base URL of smtpsink's HTTP endpoint")
		n           = flag.Int("n", 10000, "messages to send")
		concurrency = flag.Int("concurrency", 32, "parallel API requests")
		timeout     = flag.Duration("timeout", 15*time.Minute, "how long to wait for every message to settle")
	)
	flag.Parse()
	key := os.Getenv("POSTIER_API_KEY")
	if key == "" {
		fmt.Println("FAIL: POSTIER_API_KEY is not set")
		return 1
	}
	c := &client{api: *api, key: key, http: &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *concurrency, MaxConnsPerHost: *concurrency},
	}}
	resp, err := http.Post(*sink+"/reset", "", nil)
	if err != nil {
		fmt.Printf("FAIL: smtpsink unreachable: %v\n", err)
		return 1
	}
	_ = resp.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// --- send ---------------------------------------------------------------
	run := fmt.Sprintf("%x", time.Now().Unix())
	messages := make([]message, *n)
	var (
		wg     sync.WaitGroup
		failed atomic.Int64
		sent   atomic.Int64
		next   atomic.Int64
	)
	start := time.Now()
	for range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= *n {
					return
				}
				m, err := c.send(ctx, run, i, pick(i))
				if err != nil {
					failed.Add(1)
					log.Printf("send #%d: %v", i, err)
					continue
				}
				messages[i] = m
				if s := sent.Add(1); s%1000 == 0 {
					log.Printf("accepted %d/%d", s, *n)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if failed.Load() > 0 {
		fmt.Printf("\nFAIL: %d messages were not accepted\n", failed.Load())
		return 1
	}
	log.Printf("%d messages accepted in %s (%.0f/s)", *n, elapsed.Round(time.Millisecond), float64(*n)/elapsed.Seconds())

	// --- wait and verify ----------------------------------------------------
	pending := make([]int, *n)
	for i := range pending {
		pending[i] = i
	}
	problems := map[int]string{}
	for len(pending) > 0 {
		results := make([]string, len(pending))
		var next atomic.Int64
		var cwg sync.WaitGroup
		for range *concurrency {
			cwg.Add(1)
			go func() {
				defer cwg.Done()
				for {
					j := int(next.Add(1)) - 1
					if j >= len(pending) {
						return
					}
					results[j] = c.check(ctx, messages[pending[j]])
				}
			}()
		}
		cwg.Wait()

		var still []int
		for j, res := range results {
			switch {
			case res == "":
			case strings.HasPrefix(res, "pending") || strings.HasPrefix(res, "GET"):
				still = append(still, pending[j])
				problems[pending[j]] = res
			default:
				problems[pending[j]] = res // wrong for good
			}
			if res == "" {
				delete(problems, pending[j])
			}
		}
		pending = still
		if len(pending) == 0 || ctx.Err() != nil {
			break
		}
		log.Printf("%d messages not settled yet", len(pending))
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}

	// --- report -------------------------------------------------------------
	byKind := map[string][2]int{}
	for i, m := range messages {
		c := byKind[m.kind.name]
		c[0]++
		if _, bad := problems[i]; !bad {
			c[1]++
		}
		byKind[m.kind.name] = c
	}
	fmt.Printf("\n%-32s %8s %8s\n", "outcome", "sent", "exact")
	expectedAtSink := 0
	for _, k := range kinds {
		c := byKind[k.name]
		fmt.Printf("%-32s %8d %8d\n", k.name, c[0], c[1])
		if k.reaches {
			expectedAtSink += c[0]
		}
	}

	var st struct {
		Accepted   int `json:"accepted"`
		Duplicates int `json:"duplicates"`
		Recipients int `json:"recipients"`
	}
	resp, err = http.Get(*sink + "/stats")
	if err != nil {
		fmt.Printf("\nFAIL: smtpsink stats: %v\n", err)
		return 1
	}
	_ = json.NewDecoder(resp.Body).Decode(&st)
	_ = resp.Body.Close()
	fmt.Printf("\nsink: %d deliveries to %d recipients, %d expected, %d duplicates\n",
		st.Accepted, st.Recipients, expectedAtSink, st.Duplicates)

	ok := len(problems) == 0 && st.Duplicates == 0 && st.Recipients == expectedAtSink
	if !ok {
		fmt.Printf("\nFAIL: %d messages are not in their expected state\n", len(problems))
		idx := make([]int, 0, len(problems))
		for i := range problems {
			idx = append(idx, i)
		}
		sort.Ints(idx)
		for _, i := range idx[:min(len(idx), 20)] {
			fmt.Printf("  %s (%s): %s\n", messages[i].id, messages[i].rcpt, problems[i])
		}
		return 1
	}
	fmt.Printf("\nPASS: %d messages, every status and every history exact, no duplicate delivery\n", *n)
	return 0
}
