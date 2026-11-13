// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Command smtpsink is a programmable SMTP destination for tests.
//
// Mailpit accepts everything, so it cannot exercise bounces. smtpsink decides
// what to answer from the local part of the recipient:
//
//	550@…, 552-full@…   reply with that 4xx/5xx code at RCPT TO
//	data-554@…          accept the recipient, reply with the code after DATA
//	defer3-abc@…        451 for the first 3 attempts, then accept
//	oob-abc@…           accept, then send a DSN (RFC 3464) to the envelope
//	                    sender: an asynchronous, "out of band" bounce
//	fbl-abc@…           accept, then send an ARF complaint (RFC 5965)
//	slow250-abc@…       wait 250 ms, then accept
//	anything else       accept
//
// It keeps counters, exposed over HTTP, including the number of recipients
// that received the same message twice, which is what the phase 0 milestone
// measures.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/mail"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emersion/go-smtp"
)

type config struct {
	listen     string
	httpListen string
	hostname   string
	reportTo   string // host:port that receives DSNs and ARF reports
	fblAddress string // recipient of ARF reports
	reportWait time.Duration
	maxStored  int
}

// stored is a message the sink accepted, kept for inspection by tests.
type stored struct {
	From string    `json:"from"`
	To   string    `json:"to"`
	At   time.Time `json:"at"`
	Raw  string    `json:"raw"`
}

type stats struct {
	Accepted    int64            `json:"accepted"`
	Rejected    map[string]int64 `json:"rejected"` // by SMTP code
	Reports     map[string]int64 `json:"reports"`  // "dsn", "arf", "failed"
	Duplicates  int64            `json:"duplicates"`
	Recipients  int              `json:"recipients"`
	Connections int64            `json:"connections"`
}

type sink struct {
	cfg config

	mu       sync.Mutex
	st       stats
	attempts map[string]int // deferN: attempts seen, by recipient
	accepted map[string]int // accepted deliveries, by recipient + Message-ID
	last     []stored
}

func newSink(cfg config) *sink {
	s := &sink{cfg: cfg}
	s.reset()
	return s
}

func (s *sink) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st = stats{Rejected: map[string]int64{}, Reports: map[string]int64{}}
	s.attempts = map[string]int{}
	s.accepted = map[string]int{}
	s.last = nil
}

func (s *sink) snapshot() stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.st
	out.Rejected = cloneCounts(s.st.Rejected)
	out.Reports = cloneCounts(s.st.Reports)
	out.Recipients = len(s.accepted)
	return out
}

func cloneCounts(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (s *sink) countReject(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Rejected[strconv.Itoa(code)]++
}

func (s *sink) countReport(kind string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Reports[kind]++
}

// behaviour is what the sink does for one recipient.
type behaviour struct {
	rcptCode int // reply at RCPT TO
	dataCode int // reply after DATA
	deferN   int // tempfail the first N attempts
	report   string
	delay    time.Duration
}

var (
	reCode  = regexp.MustCompile(`^([45]\d\d)(?:[-+.].*)?$`)
	reData  = regexp.MustCompile(`^data-([45]\d\d)(?:[-+.].*)?$`)
	reDefer = regexp.MustCompile(`^defer(\d+)(?:[-+.].*)?$`)
	reSlow  = regexp.MustCompile(`^slow(\d+)(?:[-+.].*)?$`)
)

func behaviourFor(rcpt string) behaviour {
	local, _, _ := strings.Cut(strings.ToLower(rcpt), "@")
	var b behaviour
	switch {
	case reCode.MatchString(local):
		b.rcptCode, _ = strconv.Atoi(reCode.FindStringSubmatch(local)[1])
	case reData.MatchString(local):
		b.dataCode, _ = strconv.Atoi(reData.FindStringSubmatch(local)[1])
	case reDefer.MatchString(local):
		b.deferN, _ = strconv.Atoi(reDefer.FindStringSubmatch(local)[1])
	case reSlow.MatchString(local):
		ms, _ := strconv.Atoi(reSlow.FindStringSubmatch(local)[1])
		b.delay = time.Duration(ms) * time.Millisecond
	case local == "oob" || strings.HasPrefix(local, "oob-"):
		b.report = "dsn"
	case local == "fbl" || strings.HasPrefix(local, "fbl-"):
		b.report = "arf"
	}
	return b
}

// smtpError builds a reply with a plausible enhanced status code, close to
// what real providers send, so that bounce classification has something to
// work with.
func smtpError(code int) *smtp.SMTPError {
	e := &smtp.SMTPError{Code: code}
	switch code {
	case 550:
		e.EnhancedCode = smtp.EnhancedCode{5, 1, 1}
		e.Message = "The email account that you tried to reach does not exist"
	case 552:
		e.EnhancedCode = smtp.EnhancedCode{5, 2, 2}
		e.Message = "The recipient's inbox is out of storage space"
	case 554:
		e.EnhancedCode = smtp.EnhancedCode{5, 7, 1}
		e.Message = "Message rejected due to content restrictions"
	case 421:
		e.EnhancedCode = smtp.EnhancedCode{4, 7, 0}
		e.Message = "Try again later, closing connection"
	case 450:
		e.EnhancedCode = smtp.EnhancedCode{4, 2, 1}
		e.Message = "The user you are trying to contact is receiving mail too quickly"
	case 451:
		e.EnhancedCode = smtp.EnhancedCode{4, 3, 0}
		e.Message = "Temporary system problem, try again later"
	case 452:
		e.EnhancedCode = smtp.EnhancedCode{4, 2, 2}
		e.Message = "The recipient's inbox is out of storage space"
	default:
		e.EnhancedCode = smtp.EnhancedCode{code / 100, 0, 0}
		e.Message = "Simulated response"
	}
	return e
}

type session struct {
	sink  *sink
	from  string
	rcpts []string
}

func (s *sink) NewSession(*smtp.Conn) (smtp.Session, error) {
	s.mu.Lock()
	s.st.Connections++
	s.mu.Unlock()
	return &session{sink: s}, nil
}

func (se *session) Mail(from string, _ *smtp.MailOptions) error {
	se.from = from
	return nil
}

func (se *session) Rcpt(to string, _ *smtp.RcptOptions) error {
	b := behaviourFor(to)
	if b.rcptCode != 0 {
		se.sink.countReject(b.rcptCode)
		return smtpError(b.rcptCode)
	}
	if b.deferN > 0 {
		se.sink.mu.Lock()
		se.sink.attempts[to]++
		n := se.sink.attempts[to]
		se.sink.mu.Unlock()
		if n <= b.deferN {
			se.sink.countReject(451)
			return smtpError(451)
		}
	}
	se.rcpts = append(se.rcpts, to)
	return nil
}

func (se *session) Data(r io.Reader) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	for _, to := range se.rcpts {
		b := behaviourFor(to)
		if b.dataCode != 0 {
			se.sink.countReject(b.dataCode)
			return smtpError(b.dataCode)
		}
		if b.delay > 0 {
			time.Sleep(b.delay)
		}
	}
	for _, to := range se.rcpts {
		se.sink.accept(se.from, to, raw)
		if kind := behaviourFor(to).report; kind != "" {
			go se.sink.sendReport(kind, se.from, to, raw)
		}
	}
	return nil
}

func (se *session) Reset()        { se.from, se.rcpts = "", nil }
func (se *session) Logout() error { return nil }

func (s *sink) accept(from, to string, raw []byte) {
	key := to + "\x00" + messageID(raw)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Accepted++
	s.accepted[key]++
	if s.accepted[key] == 2 {
		s.st.Duplicates++
	}
	if s.cfg.maxStored > 0 {
		s.last = append(s.last, stored{From: from, To: to, At: time.Now().UTC(), Raw: string(raw)})
		if len(s.last) > s.cfg.maxStored {
			s.last = s.last[len(s.last)-s.cfg.maxStored:]
		}
	}
}

func messageID(raw []byte) string {
	m, err := mail.ReadMessage(strings.NewReader(string(raw)))
	if err != nil {
		return ""
	}
	return m.Header.Get("Message-ID")
}

// headersOf returns the header block of a message, without the body.
func headersOf(raw []byte) string {
	s := string(raw)
	if i := strings.Index(s, "\r\n\r\n"); i >= 0 {
		return s[:i+2]
	}
	if i := strings.Index(s, "\n\n"); i >= 0 {
		return s[:i+1]
	}
	return s
}

func (s *sink) sendReport(kind, envelopeFrom, rcpt string, raw []byte) {
	if s.cfg.reportTo == "" {
		return
	}
	time.Sleep(s.cfg.reportWait)

	var to, body string
	now := time.Now().UTC()
	switch kind {
	case "dsn":
		// A DSN goes to the envelope sender of the original message: the
		// VERP address, which is how the bounce finds its way back.
		to = envelopeFrom
		body = buildDSN(s.cfg.hostname, envelopeFrom, rcpt, raw, now)
	case "arf":
		to = s.cfg.fblAddress
		body = buildARF(s.cfg.hostname, to, envelopeFrom, rcpt, raw, now)
	}
	if to == "" {
		s.countReport("failed")
		return
	}

	// MAIL FROM:<> as RFC 3464 requires for a DSN. Retried a few times: the
	// MTA under test may be restarting, which is precisely what some tests do.
	var err error
	for attempt := range 5 {
		if err = deliver(s.cfg.reportTo, s.cfg.hostname, to, body); err == nil {
			s.countReport(kind)
			return
		}
		time.Sleep(time.Duration(attempt+1) * time.Second)
	}
	log.Printf("report %s to %s failed: %v", kind, to, err)
	s.countReport("failed")
}

func deliver(addr, helo, to, body string) error {
	c, err := smtp.Dial(addr)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	if err := c.Hello(helo); err != nil {
		return err
	}
	if err := c.SendMail("", []string{to}, strings.NewReader(body)); err != nil {
		return err
	}
	return c.Quit()
}

func buildDSN(host, envelopeFrom, rcpt string, raw []byte, now time.Time) string {
	const boundary = "smtpsink-dsn-boundary"
	date := now.Format(time.RFC1123Z)
	var b strings.Builder
	fmt.Fprintf(&b, "From: Mail Delivery Subsystem <mailer-daemon@%s>\r\n", host)
	fmt.Fprintf(&b, "To: <%s>\r\n", envelopeFrom)
	fmt.Fprintf(&b, "Subject: Delivery Status Notification (Failure)\r\n")
	fmt.Fprintf(&b, "Date: %s\r\n", date)
	fmt.Fprintf(&b, "Message-ID: <dsn-%d@%s>\r\n", now.UnixNano(), host)
	fmt.Fprintf(&b, "Auto-Submitted: auto-replied\r\n")
	fmt.Fprintf(&b, "MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: multipart/report; report-type=delivery-status; boundary=%q\r\n\r\n", boundary)

	fmt.Fprintf(&b, "--%s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n", boundary)
	fmt.Fprintf(&b, "Delivery to %s failed permanently.\r\n\r\n", rcpt)

	fmt.Fprintf(&b, "--%s\r\nContent-Type: message/delivery-status\r\n\r\n", boundary)
	fmt.Fprintf(&b, "Reporting-MTA: dns; %s\r\n", host)
	fmt.Fprintf(&b, "Arrival-Date: %s\r\n\r\n", date)
	fmt.Fprintf(&b, "Final-Recipient: rfc822; %s\r\n", rcpt)
	fmt.Fprintf(&b, "Action: failed\r\n")
	fmt.Fprintf(&b, "Status: 5.1.1\r\n")
	fmt.Fprintf(&b, "Remote-MTA: dns; %s\r\n", host)
	fmt.Fprintf(&b, "Diagnostic-Code: smtp; 550 5.1.1 The email account that you tried to reach does not exist\r\n")
	fmt.Fprintf(&b, "Last-Attempt-Date: %s\r\n\r\n", date)

	fmt.Fprintf(&b, "--%s\r\nContent-Type: text/rfc822-headers\r\n\r\n", boundary)
	b.WriteString(headersOf(raw))
	fmt.Fprintf(&b, "\r\n--%s--\r\n", boundary)
	return b.String()
}

func buildARF(host, to, envelopeFrom, rcpt string, raw []byte, now time.Time) string {
	const boundary = "smtpsink-arf-boundary"
	date := now.Format(time.RFC1123Z)
	var b strings.Builder
	fmt.Fprintf(&b, "From: Feedback Loop <fbl@%s>\r\n", host)
	fmt.Fprintf(&b, "To: <%s>\r\n", to)
	fmt.Fprintf(&b, "Subject: FW: complaint about message from %s\r\n", envelopeFrom)
	fmt.Fprintf(&b, "Date: %s\r\n", date)
	fmt.Fprintf(&b, "Message-ID: <arf-%d@%s>\r\n", now.UnixNano(), host)
	fmt.Fprintf(&b, "MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: multipart/report; report-type=feedback-report; boundary=%q\r\n\r\n", boundary)

	fmt.Fprintf(&b, "--%s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n", boundary)
	fmt.Fprintf(&b, "This is an email abuse report for a message received on %s.\r\n\r\n", date)

	fmt.Fprintf(&b, "--%s\r\nContent-Type: message/feedback-report\r\n\r\n", boundary)
	fmt.Fprintf(&b, "Feedback-Type: abuse\r\n")
	fmt.Fprintf(&b, "User-Agent: smtpsink/1.0\r\n")
	fmt.Fprintf(&b, "Version: 1\r\n")
	fmt.Fprintf(&b, "Original-Mail-From: <%s>\r\n", envelopeFrom)
	fmt.Fprintf(&b, "Original-Rcpt-To: <%s>\r\n", rcpt)
	fmt.Fprintf(&b, "Arrival-Date: %s\r\n", date)
	fmt.Fprintf(&b, "Reporting-MTA: dns; %s\r\n\r\n", host)

	fmt.Fprintf(&b, "--%s\r\nContent-Type: message/rfc822\r\nContent-Disposition: inline\r\n\r\n", boundary)
	b.Write(raw)
	fmt.Fprintf(&b, "\r\n--%s--\r\n", boundary)
	return b.String()
}

func (s *sink) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.snapshot())
	})
	mux.HandleFunc("GET /messages", func(w http.ResponseWriter, r *http.Request) {
		to := r.URL.Query().Get("to")
		s.mu.Lock()
		out := make([]stored, 0, len(s.last))
		for _, m := range s.last {
			if to == "" || strings.EqualFold(m.To, to) {
				out = append(out, m)
			}
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, _ *http.Request) {
		s.reset()
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var cfg config
	flag.StringVar(&cfg.listen, "listen", envOr("SMTPSINK_LISTEN", ":2525"), "SMTP listen address")
	flag.StringVar(&cfg.httpListen, "http", envOr("SMTPSINK_HTTP", ":8025"), "HTTP listen address (stats, stored messages)")
	flag.StringVar(&cfg.hostname, "hostname", envOr("SMTPSINK_HOSTNAME", "sink.test"), "name announced in the banner and in reports")
	flag.StringVar(&cfg.reportTo, "report-to", envOr("SMTPSINK_REPORT_TO", ""), "host:port receiving DSNs and ARF reports (empty: none sent)")
	flag.StringVar(&cfg.fblAddress, "fbl-address", envOr("SMTPSINK_FBL_ADDRESS", ""), "recipient of ARF reports")
	flag.DurationVar(&cfg.reportWait, "report-delay", 500*time.Millisecond, "delay before sending a report")
	flag.IntVar(&cfg.maxStored, "keep", 200, "accepted messages kept for GET /messages")
	flag.Parse()

	s := newSink(cfg)

	srv := smtp.NewServer(s)
	srv.Addr = cfg.listen
	srv.Domain = cfg.hostname
	srv.MaxMessageBytes = 32 << 20
	srv.MaxRecipients = 100
	srv.ReadTimeout = 60 * time.Second
	srv.WriteTimeout = 60 * time.Second
	srv.AllowInsecureAuth = true

	go func() {
		httpSrv := &http.Server{Addr: cfg.httpListen, Handler: s.httpHandler(), ReadHeaderTimeout: 5 * time.Second}
		log.Printf("smtpsink: http on %s", cfg.httpListen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	log.Printf("smtpsink: smtp on %s as %s", cfg.listen, cfg.hostname)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
