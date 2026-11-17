// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	postierv1 "github.com/SeeMyPing/postier/api/v1alpha1"
	"github.com/SeeMyPing/postier/internal/controller"
	"github.com/SeeMyPing/postier/internal/dnscheck"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/platform"
	"github.com/SeeMyPing/postier/internal/store"
)

func runOperator(ctx context.Context, args []string) error {
	var (
		c             common
		dbURL         string
		signingKey    string
		namespace     string
		baseConfigMap string
		dataConfigMap string
		dkimSecret    string
		dkimKeyDir    string
		podSelector   string
		dnsServer     string
		authoritative bool
		skipDNS       bool
		leaderElect   bool
	)
	f := platform.NewFlags("operator")
	c.register(f)
	f.String(&namespace, "namespace", "", "namespace the operator runs in, where KumoMTA's data file and DKIM keys live")
	f.String(&baseConfigMap, "base-configmap", "postier-kumomta-base", "ConfigMap holding the installation's part of KumoMTA's data file")
	f.String(&dataConfigMap, "data-configmap", "postier-kumomta-data", "ConfigMap the complete data file is written to")
	f.String(&dkimSecret, "dkim-secret", "postier-dkim", "Secret the DKIM private keys are written to")
	f.String(&dkimKeyDir, "dkim-key-dir", "/opt/kumomta/etc/dkim", "where KumoMTA mounts the DKIM Secret")
	f.String(&podSelector, "kumomta-pod-selector", "app.kubernetes.io/component=kumomta",
		"labels of KumoMTA's pods, nudged to reload their data file as soon as it changes (empty: do not)")
	f.String(&dnsServer, "dns-server", "", "recursive DNS server, host:port (default: the first of /etc/resolv.conf)")
	f.Bool(&authoritative, "dns-authoritative", true, "ask the name servers of each zone directly, instead of trusting a cache")
	f.Bool(&skipDNS, "skip-dns-verification", false, "declare every sending domain verified without checking DNS (test clusters only)")
	f.Bool(&leaderElect, "leader-elect", true, "run a leader election, so that several replicas can be deployed")
	f.Secret(&dbURL, "system-database-url")
	f.Secret(&signingKey, "signing-key")
	if err := f.Parse(args); err != nil {
		return err
	}
	if err := platform.Require(map[string]string{
		"system-database-url": dbURL, "signing-key": signingKey, "namespace": namespace,
	}); err != nil {
		return err
	}
	log, err := c.logger("operator")
	if err != nil {
		return err
	}
	ctrl.SetLogger(zap.New(zap.JSONEncoder()))
	signer, err := domain.NewSigner([]byte(signingKey))
	if err != nil {
		return err
	}
	if skipDNS {
		log.Warn("DNS verification is disabled: any declared domain can send. Do not use outside a test cluster")
	}
	podLabels, err := labels.ConvertSelectorToLabelsMap(podSelector)
	if err != nil {
		return fmt.Errorf("--kumomta-pod-selector: %w", err)
	}
	resolver, err := dnscheck.NewResolver(dnsServer, authoritative)
	if err != nil && !skipDNS {
		return err
	}

	pool, err := store.OpenSystem(ctx, dbURL, "operator")
	if err != nil {
		return err
	}
	defer pool.Close()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := postierv1.AddToScheme(scheme); err != nil {
		return err
	}
	managed := labels.SelectorFromSet(labels.Set{"app.kubernetes.io/managed-by": "postier"})
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		HealthProbeBindAddress:  c.probeAddr,
		Metrics:                 metricsserver.Options{BindAddress: "0"},
		LeaderElection:          leaderElect,
		LeaderElectionID:        "postier-operator.postier.dev",
		LeaderElectionNamespace: namespace,
		// The operator may read Secrets cluster-wide, to write API keys
		// next to their credentials. It only ever caches, and so only ever
		// sees, the ones it labelled itself.
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{&corev1.Secret{}: {Label: managed}},
		},
		// Two ConfigMaps and KumoMTA's pods, read rarely: not worth a
		// cluster-wide watch.
		Client: client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.ConfigMap{}, &corev1.Pod{}}}},
	})
	if err != nil {
		return err
	}

	cfg := &controller.Config{
		Client: mgr.GetClient(), DB: pool, Signer: signer, DNS: resolver, SkipDNS: skipDNS,
		Namespace: namespace, BaseConfigMap: baseConfigMap, DataConfigMap: dataConfigMap,
		DKIMSecret: dkimSecret, DKIMKeyDir: dkimKeyDir, KumoPodLabels: podLabels,
	}
	if err := (&controller.ProjectReconciler{Config: cfg}).SetupWithManager(mgr); err != nil {
		return err
	}
	if err := (&controller.SendingDomainReconciler{Config: cfg}).SetupWithManager(mgr); err != nil {
		return err
	}
	if err := (&controller.ApiCredentialReconciler{Config: cfg}).SetupWithManager(mgr); err != nil {
		return err
	}
	if err := mgr.Add(&controller.Renderer{Config: cfg, Interval: 30 * time.Second}); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("database", func(*http.Request) error { return pool.Ping(ctx) }); err != nil {
		return err
	}
	log.Info("starting", "namespace", namespace, "leader_election", leaderElect)
	return mgr.Start(ctrl.SetupSignalHandler())
}
