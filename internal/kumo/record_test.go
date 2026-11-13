// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package kumo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The reference files are real records, captured from the pinned KumoMTA
// version with `make capture` (see test/capture). Nothing in them is written
// by hand.

func loadRecord(t *testing.T, typ string) Record {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "records", typ+".json"))
	if err != nil {
		t.Fatal(err)
	}
	records, invalid, err := DecodeBatch(raw)
	if err != nil || len(invalid) != 0 || len(records) != 1 {
		t.Fatalf("%s: records=%d invalid=%d err=%v", typ, len(records), len(invalid), err)
	}
	return records[0]
}

func loadSession(t testing.TB) []Record {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "session.json"))
	if err != nil {
		t.Fatal(err)
	}
	records, invalid, err := DecodeBatch(raw)
	if err != nil || len(invalid) != 0 {
		t.Fatalf("session: invalid=%d err=%v", len(invalid), err)
	}
	return records
}

var allTypes = []string{
	TypeReception, TypeDelivery, TypeBounce, TypeTransientFailure, TypeExpiration,
	TypeAdminBounce, TypeOOB, TypeFeedback, TypeRejection,
}

func TestEveryRecordTypeDecodes(t *testing.T) {
	for _, typ := range allTypes {
		t.Run(typ, func(t *testing.T) {
			rec := loadRecord(t, typ)
			if rec.Type != typ {
				t.Fatalf("type = %q", rec.Type)
			}
			// A Rejection is about a message KumoMTA refused to take: it has
			// no spool id, because no message ever existed.
			if (rec.ID == "") != (typ == TypeRejection) {
				t.Errorf("spool id = %q", rec.ID)
			}
			if rec.NodeID == "" {
				t.Error("missing node id")
			}
			if at := rec.OccurredAt(); at.Year() < 2026 {
				t.Errorf("OccurredAt = %s", at)
			}
			if len(rec.DedupKey()) != 64 {
				t.Errorf("DedupKey = %q", rec.DedupKey())
			}
		})
	}
}

// TestMetadataReachesEveryOutboundRecord pins what the policy promises: the
// Postier id, the tenant and the trace come back with every record about a
// message Postier injected.
func TestMetadataReachesEveryOutboundRecord(t *testing.T) {
	for _, typ := range []string{
		TypeReception, TypeDelivery, TypeBounce, TypeTransientFailure, TypeExpiration, TypeAdminBounce,
	} {
		rec := loadRecord(t, typ)
		for _, key := range []string{MetaPostierID, MetaTenant, MetaTrace} {
			if rec.MetaString(key) == "" {
				t.Errorf("%s: metadata %q is missing", typ, key)
			}
		}
	}
}

// TestInboundRecordsCarryNoMetadata pins the finding that shaped ingest: an
// OOB or Feedback record has no metadata at all. The message has to be found
// through a signed value instead.
func TestInboundRecordsCarryNoMetadata(t *testing.T) {
	oob := loadRecord(t, TypeOOB)
	if len(oob.Meta) != 0 {
		t.Errorf("OOB metadata = %v: ingest could use it", oob.Meta)
	}
	if !strings.HasPrefix(oob.Sender, "b+") {
		t.Errorf("OOB sender = %q, want the VERP address", oob.Sender)
	}
	if strings.HasPrefix(oob.Recipient.First(), "b+") {
		t.Errorf("OOB recipient = %q, want the original recipient", oob.Recipient.First())
	}

	fb := loadRecord(t, TypeFeedback)
	if fb.FeedbackReport == nil {
		t.Fatal("Feedback record without a report")
	}
	if !strings.Contains(fb.FeedbackReport.OriginalMessageText(), "Message-ID:") {
		t.Error("Feedback report does not quote the original Message-ID")
	}
	if !strings.HasPrefix(fb.FeedbackReport.OriginalMailFrom, "b+") {
		t.Errorf("original_mail_from = %q", fb.FeedbackReport.OriginalMailFrom)
	}
}

// TestOOBTimeIsNotTakenFromTheReport: the event_time of an OOB record is the
// date written in the bounce by its sender. It must not become the partition
// key.
func TestOOBTimeIsNotTakenFromTheReport(t *testing.T) {
	oob := loadRecord(t, TypeOOB)
	if oob.CreatedTime == nil || oob.EventTime == nil {
		t.Fatal("fixture lacks created_time or event_time")
	}
	if !oob.OccurredAt().Equal(oob.CreatedTime.Truncate(time.Microsecond)) {
		t.Errorf("OccurredAt = %s, want the reception time %s", oob.OccurredAt(), oob.CreatedTime)
	}
	if !oob.ReportedAt().Equal(*oob.EventTime) {
		t.Errorf("ReportedAt = %s, want %s", oob.ReportedAt(), oob.EventTime)
	}

	// A bounce claiming to be ten years old, or from the future, lands on the
	// same reception time.
	forged := oob
	past := time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)
	forged.EventTime = &past
	if !forged.OccurredAt().Equal(oob.OccurredAt()) {
		t.Error("a forged report date moved OccurredAt")
	}
}

func TestOccurredAtHasMicrosecondPrecision(t *testing.T) {
	rec := loadRecord(t, TypeDelivery)
	at := rec.OccurredAt()
	if at.Nanosecond()%1000 != 0 {
		t.Errorf("OccurredAt %s is finer than what Postgres stores", at.Format(time.RFC3339Nano))
	}
	if at.Location() != time.UTC {
		t.Errorf("OccurredAt is in %s", at.Location())
	}
}

// TestDedupKeys is the stability check of docs/implementation-plan.md §2.3,
// on a full real session: distinct records never share a key, and a record
// decoded twice (a redelivered batch) always gets the same one.
func TestDedupKeys(t *testing.T) {
	first := loadSession(t)
	second := loadSession(t)
	if len(first) < 20 {
		t.Fatalf("session has only %d records", len(first))
	}
	seen := map[string]int{}
	for i := range first {
		key := first[i].DedupKey()
		if j, dup := seen[key]; dup {
			t.Errorf("records %d and %d share a dedup key (%s %s)", j, i, first[i].Type, first[i].ID)
		}
		seen[key] = i
		if key != second[i].DedupKey() {
			t.Errorf("record %d: key differs between two decodings", i)
		}
	}
}

func TestDedupKeySeparatesAttemptsAndRecipients(t *testing.T) {
	base := loadRecord(t, TypeTransientFailure)
	key := base.DedupKey()

	next := base
	next.NumAttempts++
	later := base.EventTime.Add(5 * time.Second)
	next.EventTime = &later
	if next.DedupKey() == key {
		t.Error("two attempts of one message share a key")
	}

	// One DSN reporting two recipients yields two OOB records with the same
	// spool id and time.
	other := base
	other.Recipient = Recipients{"someone-else@example.org"}
	if other.DedupKey() == key {
		t.Error("two recipients of one report share a key")
	}
}

func TestRecipientsAcceptsBothShapes(t *testing.T) {
	var one, many struct{ Recipient Recipients }
	if err := json.Unmarshal([]byte(`{"recipient":"a@example.org"}`), &one); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"recipient":["a@example.org","b@example.org"]}`), &many); err != nil {
		t.Fatal(err)
	}
	if one.Recipient.First() != "a@example.org" || len(many.Recipient) != 2 {
		t.Fatalf("one=%v many=%v", one.Recipient, many.Recipient)
	}
	var none Recipients
	if none.First() != "" {
		t.Fatal("First of an empty list")
	}
}

// TestOneBadRecordDoesNotSinkTheBatch: a record this version cannot decode
// is set aside; its neighbours are still processed.
func TestOneBadRecordDoesNotSinkTheBatch(t *testing.T) {
	good, err := os.ReadFile(filepath.Join("testdata", "records", "Delivery.json"))
	if err != nil {
		t.Fatal(err)
	}
	batch := "[" + string(good) + `,{"type":"Delivery","num_attempts":"three"},{"no":"type"},` + string(good) + "]"
	records, invalid, err := DecodeBatch([]byte(batch))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || len(invalid) != 2 {
		t.Fatalf("records=%d invalid=%d", len(records), len(invalid))
	}
	if !json.Valid(invalid[0].Raw) || invalid[0].Err == nil {
		t.Errorf("invalid[0] = %+v", invalid[0])
	}
}

func TestDecodeBatchRefusesWhatIsNotABatch(t *testing.T) {
	for _, body := range []string{"", "   ", "null", `"text"`, "[", `{"type":`, "42"} {
		if _, _, err := DecodeBatch([]byte(body)); err == nil {
			t.Errorf("DecodeBatch(%q) accepted", body)
		}
	}
	if _, _, err := DecodeBatch([]byte("[" + strings.Repeat("{},", MaxRecordsPerBatch) + "{}]")); err == nil {
		t.Error("oversized batch accepted")
	}
}

func TestLogHookEchoIsRecognised(t *testing.T) {
	rec := loadRecord(t, TypeDelivery)
	if rec.IsLogHookEcho() {
		t.Fatal("a real delivery flagged as an echo")
	}
	proto := ProtocolLogRecord
	rec.ReceptionProtocol = &proto
	if !rec.IsLogHookEcho() {
		t.Fatal("echo not recognised")
	}
}

// FuzzDecodeBatch feeds the decoder what the network might: the endpoint is
// authenticated, but a decoder that panics on odd input turns one bad record
// into an outage.
func FuzzDecodeBatch(f *testing.F) {
	for _, typ := range allTypes {
		raw, err := os.ReadFile(filepath.Join("testdata", "records", typ+".json"))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(raw)
		f.Add([]byte("[" + string(raw) + "]"))
	}
	f.Add([]byte(`[{"type":"OOB","recipient":["a@b.c"],"event_time":"0000-01-01T00:00:00Z"}]`))
	f.Add([]byte(`[{"type":"Feedback","feedback_report":{"original_message":[255,254]}}]`))

	f.Fuzz(func(t *testing.T, body []byte) {
		records, invalid, err := DecodeBatch(body)
		if err != nil {
			return
		}
		for i := range records {
			rec := &records[i]
			if rec.Type == "" {
				t.Fatal("a record without a type was accepted")
			}
			// None of these may panic, whatever the record holds.
			_ = rec.DedupKey()
			_ = rec.OccurredAt()
			_ = rec.ReportedAt()
			_ = rec.MetaString(MetaPostierID)
			_ = rec.IsLogHookEcho()
			_ = rec.Recipient.First()
			_ = rec.Response.EnhancedCode.String()
			_ = rec.FeedbackReport.OriginalMessageText()
		}
		for _, bad := range invalid {
			if bad.Err == nil {
				t.Fatal("an invalid record without an error")
			}
		}
	})
}
