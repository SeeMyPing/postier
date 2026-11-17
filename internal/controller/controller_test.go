// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	postierv1 "github.com/SeeMyPing/postier/api/v1alpha1"
	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/dnscheck"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/kumo"
	"github.com/SeeMyPing/postier/internal/store"
)

// These tests run the real reconcilers against a real API server (envtest)
// and a real Postgres. Only DNS is faked, which is the point: the zone below
// is edited by the tests to play the owner of a domain publishing records.
//
// They need KUBEBUILDER_ASSETS and POSTIER_TEST_DATABASE_URL, and are
// skipped otherwise. `make test-integration` sets both.

// fakeDNS is a zone the tests edit.
type fakeDNS struct {
	mu   sync.Mutex
	txt  map[string][]string
	mx   map[string][]string
	fail bool
}

func (f *fakeDNS) TXT(_ context.Context, name string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return nil, errors.New("SERVFAIL")
	}
	return append([]string(nil), f.txt[name]...), nil
}

func (f *fakeDNS) MX(_ context.Context, name string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return nil, errors.New("SERVFAIL")
	}
	return append([]string(nil), f.mx[name]...), nil
}

func (f *fakeDNS) Addrs(context.Context, string) ([]netip.Addr, error) { return nil, nil }

// publish plays the owner of a domain adding a record from the status.
func (f *fakeDNS) publish(rec postierv1.DNSRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch rec.Type {
	case "TXT":
		f.txt[rec.Name] = []string{rec.Value}
	case "MX":
		_, host, _ := strings.Cut(rec.Value, " ")
		f.mx[rec.Name] = []string{strings.TrimSuffix(host, ".")}
	}
}

func (f *fakeDNS) set(mutate func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mutate()
}

type suite struct {
	t      *testing.T
	client client.Client
	db     *pgxpool.Pool
	dns    *fakeDNS
	signer *domain.Signer
}

const systemNamespace = "postier-system"

var skipNameValidation = true

var testBase = kumo.PolicyData{
	Version: kumo.PolicyDataVersion, Hostname: "mta.postier.example",
	LogHook: kumo.LogHook{URL: "http://ingest:8081/v1/kumo/records"},
	Egress: &kumo.Egress{Sources: []kumo.EgressSource{
		{Name: "ip4-1", Address: "203.0.113.10"},
		{Name: "ip6-1", Address: "2001:db8::10"},
	}},
	Domains:        map[string]kumo.DomainData{},
	InboundDomains: map[string]bool{"fbl.postier.example": true},
}

func newSuite(t *testing.T) *suite {
	t.Helper()
	dbBase := os.Getenv("POSTIER_TEST_DATABASE_URL")
	if os.Getenv("KUBEBUILDER_ASSETS") == "" || dbBase == "" {
		t.Skip("KUBEBUILDER_ASSETS or POSTIER_TEST_DATABASE_URL is not set: operator test skipped")
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctrl.SetLogger(zap.New(zap.WriteTo(os.Stderr), zap.Level(zapErrorOnly{})))

	// --- database
	root, err := pgx.Connect(ctx, dbBase)
	if err != nil {
		t.Fatal(err)
	}
	dbName := "postier_op_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := root.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(dbBase)
	u.Path = "/" + dbName
	quiet := discardLogger()
	if err := store.Migrate(ctx, u.String(), quiet); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}

	// --- API server
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "deploy", "helm", "postier", "crds")},
		ErrorIfCRDPathMissing: true,
	}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = postierv1.AddToScheme(scheme)
	managed := labels.SelectorFromSet(labels.Set{"app.kubernetes.io/managed-by": "postier"})
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		// Each test starts its own manager, in one process: the registry of
		// controller names is global.
		Controller: config.Controller{SkipNameValidation: &skipNameValidation},
		Cache:      cache.Options{ByObject: map[client.Object]cache.ByObject{&corev1.Secret{}: {Label: managed}}},
		Client:     client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.ConfigMap{}}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	direct, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	baseJSON, _ := json.Marshal(testBase)
	for _, obj := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: systemNamespace}},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: systemNamespace, Name: "base"},
			Data:       map[string]string{BaseDataKey: string(baseJSON)},
		},
		// Stands for KumoMTA: there is no kubelet here, the pod never runs.
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: systemNamespace, Name: "kumomta-0", Labels: map[string]string{"component": "kumomta"}},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "kumod", Image: "kumomta"}}},
		},
	} {
		if err := direct.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}

	signer, _ := domain.NewSigner([]byte(strings.Repeat("k", domain.MinSigningKeyLen)))
	dns := &fakeDNS{txt: map[string][]string{}, mx: map[string][]string{}}
	cfg := &Config{
		Client: mgr.GetClient(), DB: store.WrapSystem(pool), Signer: signer, DNS: dns,
		Namespace: systemNamespace, BaseConfigMap: "base", DataConfigMap: "data",
		DKIMSecret: "dkim", DKIMKeyDir: "/keys", KumoPodLabels: map[string]string{"component": "kumomta"},
	}
	for _, r := range []interface{ SetupWithManager(ctrl.Manager) error }{
		&ProjectReconciler{Config: cfg}, &SendingDomainReconciler{Config: cfg}, &ApiCredentialReconciler{Config: cfg},
	} {
		if err := r.SetupWithManager(mgr); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("manager: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = env.Stop()
		pool.Close()
		_, _ = root.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
		_ = root.Close(context.Background())
	})
	return &suite{t: t, client: direct, db: pool, dns: dns, signer: signer}
}

func (s *suite) namespace(name string) {
	s.t.Helper()
	if err := s.client.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		s.t.Fatal(err)
	}
	s.create(&postierv1.Project{ObjectMeta: metav1.ObjectMeta{Namespace: name, Name: "shop"}})
}

func (s *suite) create(obj client.Object) {
	s.t.Helper()
	if err := s.client.Create(context.Background(), obj); err != nil {
		s.t.Fatal(err)
	}
}

func (s *suite) domain(ns, name, domainName string) *postierv1.SendingDomain {
	sd := &postierv1.SendingDomain{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       postierv1.SendingDomainSpec{ProjectRef: postierv1.ProjectReference{Name: "shop"}, Domain: domainName},
	}
	s.create(sd)
	return sd
}

// poke makes the operator reconcile now, instead of at its next DNS check.
func (s *suite) poke(obj client.Object) {
	s.t.Helper()
	key := client.ObjectKeyFromObject(obj)
	for range 5 {
		if err := s.client.Get(context.Background(), key, obj); err != nil {
			s.t.Fatal(err)
		}
		ann := obj.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		ann["test/poke"] = uuid.NewString()
		obj.SetAnnotations(ann)
		err := s.client.Update(context.Background(), obj)
		if err == nil {
			return
		}
		if !apierrors.IsConflict(err) {
			s.t.Fatal(err)
		}
	}
}

// ready waits for the Ready condition to reach a status and reason.
func (s *suite) ready(obj client.Object, conds func() []metav1.Condition, status metav1.ConditionStatus, reason string) {
	s.t.Helper()
	var last string
	s.eventually(fmt.Sprintf("%s Ready=%s (%s)", obj.GetName(), status, reason), func() bool {
		if err := s.client.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
			return false
		}
		c := meta.FindStatusCondition(conds(), postierv1.ConditionReady)
		if c == nil {
			return false
		}
		last = string(c.Status) + "/" + c.Reason
		return c.Status == status && c.Reason == reason
	}, &last)
}

func (s *suite) eventually(what string, check func() bool, last *string) {
	s.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	if last != nil {
		s.t.Fatalf("timed out waiting for %s; last seen: %s", what, *last)
	}
	s.t.Fatalf("timed out waiting for %s", what)
}

// row returns whether a live row exists for the domain, and if it is verified.
func (s *suite) row(domainName string) (exists, verified bool) {
	s.t.Helper()
	var verifiedAt *time.Time
	err := s.db.QueryRow(context.Background(),
		"SELECT verified_at FROM sending_domains WHERE domain = $1 AND deleted_at IS NULL", domainName).Scan(&verifiedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false
	}
	if err != nil {
		s.t.Fatal(err)
	}
	return true, verifiedAt != nil
}

func (s *suite) kumoData() kumo.PolicyData {
	s.t.Helper()
	var cm corev1.ConfigMap
	var data kumo.PolicyData
	if err := s.client.Get(context.Background(), types.NamespacedName{Namespace: systemNamespace, Name: "data"}, &cm); err != nil {
		return data
	}
	_ = json.Unmarshal([]byte(cm.Data[DataKey]), &data)
	return data
}

func sdConds(sd *postierv1.SendingDomain) func() []metav1.Condition {
	return func() []metav1.Condition { return sd.Status.Conditions }
}

// TestSendingDomainFromDeclarationToSending follows a domain through every
// step its owner goes through.
func TestSendingDomainFromDeclarationToSending(t *testing.T) {
	s := newSuite(t)
	s.namespace("acme")
	const name = "notifications.acme.example"
	sd := s.domain("acme", "notifications", name)

	// 1. Declared. Nothing is published: the status says what to publish,
	// and the database knows nothing of the domain, so that a mere
	// declaration cannot reserve a name.
	s.ready(sd, sdConds(sd), metav1.ConditionFalse, "OwnershipNotVerified")
	records := map[string]postierv1.DNSRecord{}
	for _, r := range sd.Status.RequiredRecords {
		records[r.Type+" "+r.Name] = r
	}
	challenge := records["TXT _postier-challenge."+name]
	dkim := records["TXT pst1._domainkey."+name]
	mx := records["MX bounce."+name]
	spf := records["TXT bounce."+name]
	if challenge.Value != "pst-verify="+s.signer.ChallengeToken("acme", name) || !strings.HasPrefix(dkim.Value, "v=DKIM1; k=rsa; p=MII") ||
		mx.Value != "10 mta.postier.example." {
		t.Fatalf("required records: %+v", sd.Status.RequiredRecords)
	}
	// Both address families of the installation: the lesson of the first
	// real send, where IPv6 had been forgotten.
	if spf.Value != "v=spf1 ip4:203.0.113.10 ip6:2001:db8::10 -all" {
		t.Errorf("SPF to publish: %q", spf.Value)
	}
	if exists, _ := s.row(name); exists {
		t.Fatal("an unproven domain was written to the database")
	}

	// 2. Ownership proven. The domain is registered, but cannot send yet.
	s.dns.publish(challenge)
	s.poke(sd)
	s.ready(sd, sdConds(sd), metav1.ConditionFalse, "KeyMissing")
	if exists, verified := s.row(name); !exists || verified {
		t.Fatalf("after the challenge: exists=%v verified=%v", exists, verified)
	}
	if _, listed := s.kumoData().Domains[name]; listed {
		t.Fatal("KumoMTA was told to sign for a domain that is not ready")
	}

	// 3. DKIM and MX published, but an SPF record that forgets IPv6.
	s.dns.publish(dkim)
	s.dns.publish(mx)
	s.dns.publish(postierv1.DNSRecord{Type: "TXT", Name: spf.Name, Value: "v=spf1 ip4:203.0.113.10 -all"})
	s.poke(sd)
	s.ready(sd, sdConds(sd), metav1.ConditionFalse, "SPFDoesNotCoverSource")

	// 4. Everything in place.
	s.dns.publish(spf)
	s.poke(sd)
	s.ready(sd, sdConds(sd), metav1.ConditionTrue, "Verified")
	if _, verified := s.row(name); !verified {
		t.Fatal("a ready domain is not verified in the database")
	}
	// The data file is written right after the status.
	s.eventually("the domain to reach KumoMTA's data", func() bool {
		_, listed := s.kumoData().Domains[name]
		return listed
	}, nil)
	data := s.kumoData()
	if got := data.Domains[name]; got.Selector != "pst1" || got.KeyFile != "/keys/"+name+".pst1.key" {
		t.Errorf("KumoMTA data for the domain: %+v", got)
	}
	if !data.InboundDomains["bounce."+name] || !data.InboundDomains["fbl.postier.example"] {
		t.Errorf("inbound domains: %v", data.InboundDomains)
	}
	// KumoMTA's pod is told that its data file changed, so that the kubelet
	// refreshes it now instead of a minute or two later.
	s.eventually("KumoMTA's pod to be nudged", func() bool {
		var pod corev1.Pod
		err := s.client.Get(context.Background(), types.NamespacedName{Namespace: systemNamespace, Name: "kumomta-0"}, &pod)
		return err == nil && pod.Annotations[DataHashAnnotation] != ""
	}, nil)
	// DMARC is reported, without blocking.
	if c := meta.FindStatusCondition(sd.Status.Conditions, postierv1.ConditionDMARCPublished); c == nil || c.Status != metav1.ConditionFalse {
		t.Errorf("DMARC condition: %+v", c)
	}

	// 5. The resolver breaks. Nothing is known, so nothing changes: an
	// outage must not stop a domain that is sending.
	s.dns.set(func() { s.dns.fail = true })
	s.poke(sd)
	time.Sleep(2 * time.Second)
	s.ready(sd, sdConds(sd), metav1.ConditionTrue, "Verified")
	if _, verified := s.row(name); !verified {
		t.Fatal("a DNS outage unverified the domain")
	}

	// 6. The DKIM record is really gone. The domain stops sending.
	s.dns.set(func() { s.dns.fail = false; delete(s.dns.txt, dkim.Name) })
	s.poke(sd)
	s.ready(sd, sdConds(sd), metav1.ConditionFalse, "KeyMissing")
	if _, verified := s.row(name); verified {
		t.Fatal("the domain is still verified without its DKIM record")
	}
	s.eventually("the domain to leave KumoMTA's data", func() bool {
		_, listed := s.kumoData().Domains[name]
		return !listed
	}, nil)

	// 7. Deleted. The name is free again and the key is destroyed.
	if err := s.client.Delete(context.Background(), sd); err != nil {
		t.Fatal(err)
	}
	s.eventually("the domain to be retired", func() bool {
		exists, _ := s.row(name)
		var secret corev1.Secret
		_ = s.client.Get(context.Background(), types.NamespacedName{Namespace: systemNamespace, Name: "dkim"}, &secret)
		return !exists && len(secret.Data) == 0
	}, nil)
}

// TestDomainBelongsToWhoProvesIt: two tenants declare the same domain. Each
// gets its own challenge; only the one who can publish it gets the domain.
func TestDomainBelongsToWhoProvesIt(t *testing.T) {
	s := newSuite(t)
	s.namespace("squatter")
	s.namespace("owner")
	const name = "brand.example"

	squat := s.domain("squatter", "grab", name)
	s.ready(squat, sdConds(squat), metav1.ConditionFalse, "OwnershipNotVerified")

	mine := s.domain("owner", "brand", name)
	s.ready(mine, sdConds(mine), metav1.ConditionFalse, "OwnershipNotVerified")
	if s.signer.ChallengeToken("squatter", name) == s.signer.ChallengeToken("owner", name) {
		t.Fatal("both tenants were given the same challenge")
	}
	for _, r := range mine.Status.RequiredRecords {
		s.dns.publish(r)
	}
	s.poke(mine)
	s.ready(mine, sdConds(mine), metav1.ConditionTrue, "Verified")

	// The squatter declared first, and still has nothing.
	// It sees a challenge record, but not its own.
	s.poke(squat)
	s.eventually("the squatter to see a foreign challenge", func() bool {
		_ = s.client.Get(context.Background(), client.ObjectKeyFromObject(squat), squat)
		c := meta.FindStatusCondition(squat.Status.Conditions, postierv1.ConditionOwnershipVerified)
		return c != nil && c.Status == metav1.ConditionFalse && c.Reason == "ChallengeMismatch"
	}, nil)
	s.ready(squat, sdConds(squat), metav1.ConditionFalse, "OwnershipNotVerified")
	var tenant string
	if err := s.db.QueryRow(context.Background(), `
		SELECT t.name FROM sending_domains d JOIN tenants t ON t.id = d.tenant_id
		WHERE d.domain = $1 AND d.deleted_at IS NULL`, name).Scan(&tenant); err != nil || tenant != "owner" {
		t.Fatalf("the domain belongs to %q, %v", tenant, err)
	}
	// Deleting the loser must not retire the winner's domain.
	if err := s.client.Delete(context.Background(), squat); err != nil {
		t.Fatal(err)
	}
	s.eventually("the squatter's resource to go", func() bool {
		return apierrors.IsNotFound(s.client.Get(context.Background(), client.ObjectKeyFromObject(squat), squat))
	}, nil)
	if _, verified := s.row(name); !verified {
		t.Fatal("deleting another tenant's resource retired the domain")
	}
}

func credConds(c *postierv1.ApiCredential) func() []metav1.Condition {
	return func() []metav1.Condition { return c.Status.Conditions }
}

func TestApiCredentialLifecycle(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()
	s.namespace("acme")
	auth := accept.NewAuthenticator(store.WrapApp(s.db))

	cred := &postierv1.ApiCredential{
		ObjectMeta: metav1.ObjectMeta{Namespace: "acme", Name: "backend"},
		Spec: postierv1.ApiCredentialSpec{
			ProjectRef: postierv1.ProjectReference{Name: "shop"},
			Scopes:     []postierv1.Scope{"send", "suppressions"},
		},
	}
	s.create(cred)
	s.ready(cred, credConds(cred), metav1.ConditionTrue, "Issued")

	readKey := func() string {
		var secret corev1.Secret
		if err := s.client.Get(ctx, types.NamespacedName{Namespace: "acme", Name: "backend"}, &secret); err != nil {
			return ""
		}
		return string(secret.Data[postierv1.SecretKeyAPIKey])
	}
	key := readKey()
	if !strings.HasPrefix(key, "pst_live_"+cred.Status.Prefix+"_") {
		t.Fatalf("key %q does not match the status prefix %q", key[:min(len(key), 20)], cred.Status.Prefix)
	}
	p, err := auth.Authenticate(ctx, key)
	if err != nil || !p.Can(accept.ScopeSend) || !p.Can(accept.ScopeSuppressions) || p.Can(accept.ScopeRead) {
		t.Fatalf("authenticating with the issued key: %+v, %v", p, err)
	}
	// Only the hash is in the database.
	var stored int
	_ = s.db.QueryRow(ctx, "SELECT count(*) FROM api_credentials WHERE secret_hash = $1::bytea OR prefix = $2", []byte(key), key).Scan(&stored)
	if stored != 0 {
		t.Fatal("the key itself is in the database")
	}

	// Scopes follow the spec.
	if err := s.client.Get(ctx, client.ObjectKeyFromObject(cred), cred); err != nil {
		t.Fatal(err)
	}
	cred.Spec.Scopes = []postierv1.Scope{"read"}
	if err := s.client.Update(ctx, cred); err != nil {
		t.Fatal(err)
	}
	s.eventually("scopes to be updated", func() bool {
		p, err := auth.Authenticate(ctx, key)
		return err == nil && p.Can(accept.ScopeRead) && !p.Can(accept.ScopeSend)
	}, nil)

	// Deleting the Secret rotates the key: the old one dies.
	var secret corev1.Secret
	_ = s.client.Get(ctx, types.NamespacedName{Namespace: "acme", Name: "backend"}, &secret)
	if err := s.client.Delete(ctx, &secret); err != nil {
		t.Fatal(err)
	}
	s.eventually("a new key", func() bool { k := readKey(); return k != "" && k != key }, nil)
	rotated := readKey()
	s.eventually("the old key to stop working", func() bool {
		_, oldErr := auth.Authenticate(ctx, key)
		_, newErr := auth.Authenticate(ctx, rotated)
		return oldErr != nil && newErr == nil
	}, nil)

	// Deleting the resource revokes it.
	if err := s.client.Delete(ctx, cred); err != nil {
		t.Fatal(err)
	}
	s.eventually("the key to be revoked", func() bool {
		_, err := auth.Authenticate(ctx, rotated)
		return err != nil && apierrors.IsNotFound(s.client.Get(ctx, client.ObjectKeyFromObject(cred), cred))
	}, nil)
}

// TestApiCredentialDoesNotAdoptForeignSecrets: if the operator took over a
// Secret that already exists, whoever can write a Secret could mint a key,
// or have their own data replaced.
func TestApiCredentialDoesNotAdoptForeignSecrets(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()
	s.namespace("acme")
	forged, _ := accept.GenerateKey("live")
	s.create(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "acme", Name: "taken"},
		Data:       map[string][]byte{postierv1.SecretKeyAPIKey: []byte(forged.Token)},
	})
	cred := &postierv1.ApiCredential{
		ObjectMeta: metav1.ObjectMeta{Namespace: "acme", Name: "taken"},
		Spec:       postierv1.ApiCredentialSpec{ProjectRef: postierv1.ProjectReference{Name: "shop"}},
	}
	s.create(cred)
	s.ready(cred, credConds(cred), metav1.ConditionFalse, "SecretConflict")

	if _, err := accept.NewAuthenticator(store.WrapApp(s.db)).Authenticate(ctx, forged.Token); err == nil {
		t.Fatal("the content of a pre-existing Secret became a valid API key")
	}
	var secret corev1.Secret
	_ = s.client.Get(ctx, types.NamespacedName{Namespace: "acme", Name: "taken"}, &secret)
	if string(secret.Data[postierv1.SecretKeyAPIKey]) != forged.Token {
		t.Fatal("the pre-existing Secret was overwritten")
	}
}

func TestResourcesWaitForTheirProject(t *testing.T) {
	s := newSuite(t)
	if err := s.client.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "early"}}); err != nil {
		t.Fatal(err)
	}
	cred := &postierv1.ApiCredential{
		ObjectMeta: metav1.ObjectMeta{Namespace: "early", Name: "backend"},
		Spec:       postierv1.ApiCredentialSpec{ProjectRef: postierv1.ProjectReference{Name: "shop"}},
	}
	s.create(cred)
	s.ready(cred, credConds(cred), metav1.ConditionFalse, "ProjectNotReady")
	// The order of a `kubectl apply -f` must not matter.
	s.create(&postierv1.Project{ObjectMeta: metav1.ObjectMeta{Namespace: "early", Name: "shop"}})
	s.poke(cred)
	s.ready(cred, credConds(cred), metav1.ConditionTrue, "Issued")
}

// TestCRDValidation: what the API server refuses on its own.
func TestCRDValidation(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()
	s.namespace("acme")
	for _, bad := range []string{"Acme.Example", "acme", "-acme.example", "acme..example", "bücher.example", "acme.example."} {
		sd := &postierv1.SendingDomain{
			ObjectMeta: metav1.ObjectMeta{Namespace: "acme", Name: "bad-" + uuid.NewString()[:8]},
			Spec:       postierv1.SendingDomainSpec{ProjectRef: postierv1.ProjectReference{Name: "shop"}, Domain: bad},
		}
		if err := s.client.Create(ctx, sd); err == nil {
			t.Errorf("domain %q was accepted", bad)
		}
	}
	sd := s.domain("acme", "ok", "acme.example")
	if sd.Spec.DKIM.Selector != "pst1" || sd.Spec.ReturnPath.Subdomain != "bounce" {
		t.Errorf("defaults: %+v", sd.Spec)
	}
	// A domain is not renamed: it is another domain.
	sd.Spec.Domain = "other.example"
	if err := s.client.Update(ctx, sd); err == nil {
		t.Error("the domain of a SendingDomain was changed")
	}
	cred := &postierv1.ApiCredential{
		ObjectMeta: metav1.ObjectMeta{Namespace: "acme", Name: "c"},
		Spec: postierv1.ApiCredentialSpec{
			ProjectRef: postierv1.ProjectReference{Name: "shop"}, Scopes: []postierv1.Scope{"root"},
		},
	}
	if err := s.client.Create(ctx, cred); err == nil {
		t.Error("an unknown scope was accepted")
	}
}

var _ dnscheck.Resolver = (*fakeDNS)(nil)
