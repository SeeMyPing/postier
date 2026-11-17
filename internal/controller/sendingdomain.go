// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	postierv1 "github.com/SeeMyPing/postier/api/v1alpha1"
	"github.com/SeeMyPing/postier/internal/dnscheck"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/kumo"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// SendingDomainReconciler takes a domain from declaration to sending:
//
//  1. it generates the DKIM key;
//  2. it lists, in the status, the DNS records to publish;
//  3. it checks them, straight at the zone's name servers;
//  4. once ownership is proven, it registers the domain in the database,
//     where its uniqueness across tenants is enforced;
//  5. once everything passes, it marks the domain verified, which is what
//     lets the API accept mail from it, and adds it to KumoMTA's data file.
type SendingDomainReconciler struct {
	*Config
}

const dkimKeyBits = 2048

func dkimSecretKey(domainName, selector string) string {
	return domainName + "." + selector + ".key"
}

func returnPathOf(sd *postierv1.SendingDomain) string {
	return sd.Spec.ReturnPath.Subdomain + "." + sd.Spec.Domain
}

// Reconcile implements reconcile.Reconciler.
func (r *SendingDomainReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var sd postierv1.SendingDomain
	if err := r.Client.Get(ctx, req.NamespacedName, &sd); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !sd.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &sd)
	}
	if controllerutil.AddFinalizer(&sd, Finalizer) {
		if err := r.Client.Update(ctx, &sd); err != nil {
			return ctrl.Result{}, err
		}
	}

	wasReady := meta.IsStatusConditionTrue(sd.Status.Conditions, postierv1.ConditionReady)
	result, err := r.reconcile(ctx, &sd, wasReady)
	sd.Status.ObservedGeneration = sd.Generation
	if uerr := r.Client.Status().Update(ctx, &sd); uerr != nil && err == nil {
		err = uerr
	}
	// The data file follows the set of ready domains.
	if rerr := r.RenderKumoData(ctx); rerr != nil && err == nil {
		err = rerr
	}
	return result, err
}

func (r *SendingDomainReconciler) notReady(sd *postierv1.SendingDomain, reason, message string) {
	setCondition(&sd.Status.Conditions, sd.Generation, postierv1.ConditionReady, metav1.ConditionFalse, reason, message)
}

func (r *SendingDomainReconciler) reconcile(ctx context.Context, sd *postierv1.SendingDomain, wasReady bool) (ctrl.Result, error) {
	ids, err := r.resolveProject(ctx, sd.Namespace, sd.Spec.ProjectRef.Name)
	if errors.Is(err, errProjectNotReady) {
		r.notReady(sd, "ProjectNotReady", fmt.Sprintf("project %q does not exist yet", sd.Spec.ProjectRef.Name))
		return ctrl.Result{RequeueAfter: retryDependency}, nil
	}
	if err != nil {
		r.notReady(sd, "SyncFailed", "could not read the project")
		return ctrl.Result{}, err
	}

	publicKey, err := r.ensureDKIMKey(ctx, sd)
	if err != nil {
		r.notReady(sd, "KeyFailed", "could not generate or read the DKIM key")
		return ctrl.Result{}, err
	}

	base, err := r.baseData(ctx)
	if err != nil {
		r.notReady(sd, "ConfigurationMissing", "the base configuration of KumoMTA could not be read")
		return ctrl.Result{}, err
	}
	token := r.Signer.ChallengeToken(sd.Namespace, sd.Spec.Domain)
	sd.Status.RequiredRecords = requiredRecords(sd, token, publicKey, base)

	checks, lookupErr := r.checkDNS(ctx, sd, token, publicKey, base)
	now := metav1.NewTime(r.now())
	sd.Status.LastCheckTime = &now
	if lookupErr != nil {
		// Nothing was learnt: a resolver outage must not unverify a domain
		// that is sending. Conditions stay as they were.
		log.FromContext(ctx).Info("DNS lookup failed, keeping the previous state", "domain", sd.Spec.Domain, "err", lookupErr.Error())
		if !wasReady {
			r.notReady(sd, "LookupFailed", "DNS could not be queried: "+lookupErr.Error())
		}
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	for typ, res := range checks {
		setCondition(&sd.Status.Conditions, sd.Generation, typ, boolStatus(res.OK), res.Reason, res.Message)
	}

	// The database is only told about a domain whose ownership is proven:
	// its unique index is first come, first served, and must not be held by
	// someone who merely declared a name.
	if !checks[postierv1.ConditionOwnershipVerified].OK {
		r.notReady(sd, "OwnershipNotVerified", checks[postierv1.ConditionOwnershipVerified].Message)
		return ctrl.Result{RequeueAfter: recheckPending}, nil
	}
	ready := checks[postierv1.ConditionDKIMPublished].OK && checks[postierv1.ConditionReturnPathValid].OK
	var verifiedAt *time.Time
	if ready {
		t := r.now()
		verifiedAt = &t
	}
	_, err = r.DB.Q().UpsertSendingDomain(ctx, sqlcgen.UpsertSendingDomainParams{
		ID: domain.NewID(), TenantID: ids.TenantID, ProjectID: ids.ProjectID,
		Domain: sd.Spec.Domain, ReturnPathDomain: returnPathOf(sd),
		DkimSelector: sd.Spec.DKIM.Selector, VerifiedAt: verifiedAt,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		r.notReady(sd, "DomainConflict", "this domain is already registered by another tenant")
		return ctrl.Result{RequeueAfter: recheckPending}, nil
	}
	if err != nil {
		r.notReady(sd, "SyncFailed", "could not write the domain to the database")
		return ctrl.Result{}, err
	}

	if !ready {
		for _, typ := range []string{postierv1.ConditionDKIMPublished, postierv1.ConditionReturnPathValid} {
			if !checks[typ].OK {
				r.notReady(sd, checks[typ].Reason, checks[typ].Message)
				break
			}
		}
		return ctrl.Result{RequeueAfter: recheckPending}, nil
	}
	setCondition(&sd.Status.Conditions, sd.Generation, postierv1.ConditionReady, metav1.ConditionTrue,
		"Verified", "DNS is in place: the domain can send")
	return ctrl.Result{RequeueAfter: recheckReady}, nil
}

// checkDNS runs the four checks. The error is a lookup failure: no verdict.
func (r *SendingDomainReconciler) checkDNS(ctx context.Context, sd *postierv1.SendingDomain, token, publicKey string, base *kumo.PolicyData) (map[string]dnscheck.Result, error) {
	if r.SkipDNS {
		skipped := dnscheck.Result{OK: true, Reason: "CheckSkipped", Message: "DNS verification is disabled on this installation"}
		return map[string]dnscheck.Result{
			postierv1.ConditionOwnershipVerified: skipped,
			postierv1.ConditionDKIMPublished:     skipped,
			postierv1.ConditionReturnPathValid:   skipped,
			postierv1.ConditionDMARCPublished:    skipped,
		}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	out := map[string]dnscheck.Result{}
	var err error
	if out[postierv1.ConditionOwnershipVerified], err = dnscheck.CheckChallenge(ctx, r.DNS, sd.Spec.Domain, token); err != nil {
		return nil, err
	}
	if out[postierv1.ConditionDKIMPublished], err = dnscheck.CheckDKIM(ctx, r.DNS, sd.Spec.DKIM.Selector, sd.Spec.Domain, publicKey); err != nil {
		return nil, err
	}
	if out[postierv1.ConditionReturnPathValid], err = dnscheck.CheckReturnPath(ctx, r.DNS, returnPathOf(sd), base.Hostname, egressAddrs(base)); err != nil {
		return nil, err
	}
	if out[postierv1.ConditionDMARCPublished], err = dnscheck.CheckDMARC(ctx, r.DNS, sd.Spec.Domain); err != nil {
		return nil, err
	}
	return out, nil
}

func egressAddrs(base *kumo.PolicyData) []netip.Addr {
	if base.Egress == nil {
		return nil
	}
	var out []netip.Addr
	for _, src := range base.Egress.Sources {
		if a, err := netip.ParseAddr(src.Address); err == nil {
			out = append(out, a)
		}
	}
	return out
}

// requiredRecords is what the owner of the domain has to publish.
func requiredRecords(sd *postierv1.SendingDomain, token, publicKey string, base *kumo.PolicyData) []postierv1.DNSRecord {
	spf := "v=spf1"
	sources := egressAddrs(base)
	for _, a := range sources {
		if a.Is4() {
			spf += " ip4:" + a.String()
		} else {
			spf += " ip6:" + a.String()
		}
	}
	if len(sources) == 0 {
		// The installation does not pin its sending addresses, so they are
		// not known here. "-all" alone would forbid everything.
		spf += " ip4:<sending IPv4> ip6:<sending IPv6, if any>"
	}
	spf += " -all"
	rp := returnPathOf(sd)
	return []postierv1.DNSRecord{
		{Type: "TXT", Name: dnscheck.ChallengeName(sd.Spec.Domain), Value: dnscheck.ChallengePrefix + token,
			Purpose: "Proves that you control the domain"},
		{Type: "TXT", Name: dnscheck.DKIMName(sd.Spec.DKIM.Selector, sd.Spec.Domain), Value: dnscheck.DKIMValue(publicKey),
			Purpose: "DKIM public key"},
		{Type: "MX", Name: rp, Value: "10 " + base.Hostname + ".",
			Purpose: "Routes bounces back to the platform"},
		{Type: "TXT", Name: rp, Value: spf,
			Purpose: "SPF: authorises the platform's sending addresses"},
		{Type: "TXT", Name: "_dmarc." + sd.Spec.Domain, Value: "v=DMARC1; p=none",
			Purpose: "DMARC policy (recommended; required by Gmail and Yahoo for bulk senders)"},
	}
}

// ensureDKIMKey returns the public key of the domain (base64 of its DER),
// generating the key pair on first use. Private keys live in one Secret of
// the operator's namespace, which KumoMTA mounts; tenants never see them.
func (r *SendingDomainReconciler) ensureDKIMKey(ctx context.Context, sd *postierv1.SendingDomain) (string, error) {
	secret, err := r.dkimSecret(ctx)
	if err != nil {
		return "", err
	}
	name := dkimSecretKey(sd.Spec.Domain, sd.Spec.DKIM.Selector)
	if pemBytes, ok := secret.Data[name]; ok {
		return publicKeyOf(pemBytes)
	}

	key, err := rsa.GenerateKey(rand.Reader, dkimKeyBits)
	if err != nil {
		return "", err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data[name] = pemBytes
	// A conflict here means another reconcile added a key meanwhile: the
	// update fails, the request is retried, nothing is overwritten.
	if err := r.Client.Update(ctx, secret); err != nil {
		return "", err
	}
	return publicKeyOf(pemBytes)
}

func publicKeyOf(pemBytes []byte) (string, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return "", errors.New("DKIM key is not PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", err
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return "", errors.New("DKIM key is not an RSA key")
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(der), nil
}

// dkimSecret returns the Secret of DKIM keys, creating it empty if needed.
func (r *Config) dkimSecret(ctx context.Context) (*corev1.Secret, error) {
	var secret corev1.Secret
	key := types.NamespacedName{Namespace: r.Namespace, Name: r.DKIMSecret}
	err := r.Client.Get(ctx, key, &secret)
	if apierrors.IsNotFound(err) {
		secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: key.Namespace, Name: key.Name,
				Labels: map[string]string{"app.kubernetes.io/managed-by": "postier"},
			},
			Type: corev1.SecretTypeOpaque,
		}
		err = r.Client.Create(ctx, &secret)
	}
	return &secret, err
}

// finalize undoes a deleted domain: it stops sending, its name is released,
// its key is destroyed.
func (r *SendingDomainReconciler) finalize(ctx context.Context, sd *postierv1.SendingDomain) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(sd, Finalizer) {
		return ctrl.Result{}, nil
	}
	ids, err := r.ensureProject(ctx, sd.Namespace, sd.Spec.ProjectRef.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Scoped to the tenant: deleting a resource that lost a conflict must
	// not retire the domain of its rightful owner.
	if _, err := r.DB.Q().RetireSendingDomain(ctx, sqlcgen.RetireSendingDomainParams{
		Domain: sd.Spec.Domain, TenantID: ids.TenantID,
	}); err != nil {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(sd, Finalizer)
	if err := r.Client.Update(ctx, sd); err != nil {
		return ctrl.Result{}, err
	}
	// Renders without this domain, and drops its key.
	return ctrl.Result{}, r.RenderKumoData(ctx)
}

// SetupWithManager registers the reconciler.
func (r *SendingDomainReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&postierv1.SendingDomain{}).Complete(r)
}
