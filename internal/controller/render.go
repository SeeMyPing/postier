// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	postierv1 "github.com/SeeMyPing/postier/api/v1alpha1"
	"github.com/SeeMyPing/postier/internal/kumo"
)

// Keys of the two ConfigMaps.
const (
	BaseDataKey = "base.json"
	DataKey     = "data.json"
)

// baseData reads the part of KumoMTA's data file that the installation
// provides: hostname, listeners, log hook, egress, routes. Domains are not
// in it; they come from SendingDomain resources.
func (r *Config) baseData(ctx context.Context) (*kumo.PolicyData, error) {
	var cm corev1.ConfigMap
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.BaseConfigMap}, &cm); err != nil {
		return nil, fmt.Errorf("base configuration: %w", err)
	}
	var base kumo.PolicyData
	dec := json.NewDecoder(strings.NewReader(cm.Data[BaseDataKey]))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&base); err != nil {
		return nil, fmt.Errorf("base configuration: %s: %w", BaseDataKey, err)
	}
	return &base, nil
}

// RenderKumoData writes KumoMTA's data file from the base configuration and
// the sending domains that are ready, and prunes the DKIM keys of domains
// that no longer exist.
//
// The file is only written when it changes. KumoMTA re-reads it on its own:
// a new domain never restarts the MTA.
func (r *Config) RenderKumoData(ctx context.Context) error {
	base, err := r.baseData(ctx)
	if err != nil {
		return err
	}
	var domains postierv1.SendingDomainList
	if err := r.Client.List(ctx, &domains); err != nil {
		return err
	}

	data := *base
	data.Domains = map[string]kumo.DomainData{}
	inbound := map[string]bool{}
	for d, ok := range base.InboundDomains {
		inbound[d] = ok
	}
	keep := map[string]bool{}
	for i := range domains.Items {
		sd := &domains.Items[i]
		if !sd.DeletionTimestamp.IsZero() {
			continue
		}
		keyName := dkimSecretKey(sd.Spec.Domain, sd.Spec.DKIM.Selector)
		// A key is kept as long as its resource exists, ready or not: its
		// public half may already be published.
		keep[keyName] = true
		if !meta.IsStatusConditionTrue(sd.Status.Conditions, postierv1.ConditionReady) {
			continue
		}
		// Two resources can name the same domain; only one is ever Ready,
		// the database sees to that.
		data.Domains[sd.Spec.Domain] = kumo.DomainData{
			Selector: sd.Spec.DKIM.Selector,
			KeyFile:  r.DKIMKeyDir + "/" + keyName,
		}
		inbound[returnPathOf(sd)] = true
	}
	data.InboundDomains = inbound

	rendered, err := data.Render()
	if err != nil {
		return err
	}
	if err := r.writeData(ctx, rendered); err != nil {
		return err
	}
	return r.pruneDKIMKeys(ctx, keep)
}

func (r *Config) writeData(ctx context.Context, rendered []byte) error {
	key := types.NamespacedName{Namespace: r.Namespace, Name: r.DataConfigMap}
	var cm corev1.ConfigMap
	err := r.Client.Get(ctx, key, &cm)
	if apierrors.IsNotFound(err) {
		cm = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: key.Namespace, Name: key.Name,
				Labels: map[string]string{"app.kubernetes.io/managed-by": "postier"},
			},
			Data: map[string]string{DataKey: string(rendered)},
		}
		return r.Client.Create(ctx, &cm)
	}
	if err != nil {
		return err
	}
	if bytes.Equal([]byte(cm.Data[DataKey]), rendered) {
		return nil
	}
	cm.Data = map[string]string{DataKey: string(rendered)}
	log.FromContext(ctx).Info("KumoMTA data file updated")
	if err := r.Client.Update(ctx, &cm); err != nil {
		return err
	}
	return r.nudgeKumoPods(ctx, rendered)
}

// DataHashAnnotation is set on KumoMTA's pods when the data file changes.
const DataHashAnnotation = "postier.dev/data-hash"

// nudgeKumoPods makes the kubelet refresh the mounted data file now.
//
// A ConfigMap volume is only re-synced periodically, a minute or two after
// the ConfigMap changes. Measured on kind: a domain was Ready, and KumoMTA
// kept answering "domain not configured" for 100 seconds. Any change to a
// pod's annotations triggers the sync at once, without restarting anything.
// It is best effort: without it the file still arrives, later.
func (r *Config) nudgeKumoPods(ctx context.Context, rendered []byte) error {
	if len(r.KumoPodLabels) == 0 {
		return nil
	}
	var pods corev1.PodList
	if err := r.Client.List(ctx, &pods, client.InNamespace(r.Namespace), client.MatchingLabels(r.KumoPodLabels)); err != nil {
		return fmt.Errorf("listing KumoMTA pods: %w", err)
	}
	sum := sha256.Sum256(rendered)
	hash := hex.EncodeToString(sum[:8])
	for i := range pods.Items {
		pod := &pods.Items[i]
		patch := client.MergeFrom(pod.DeepCopy())
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[DataHashAnnotation] = hash
		if err := r.Client.Patch(ctx, pod, patch); err != nil {
			return fmt.Errorf("annotating pod %s: %w", pod.Name, err)
		}
	}
	return nil
}

func (r *Config) pruneDKIMKeys(ctx context.Context, keep map[string]bool) error {
	secret, err := r.dkimSecret(ctx)
	if err != nil {
		return err
	}
	changed := false
	for name := range secret.Data {
		if !keep[name] {
			delete(secret.Data, name)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return r.Client.Update(ctx, secret)
}

// Renderer re-renders the data file periodically. Reconciles already do it
// on every change of a domain; this covers a change of the base
// configuration (a chart upgrade), which no reconcile sees.
type Renderer struct {
	*Config
	Interval time.Duration
}

// Start implements manager.Runnable.
func (r *Renderer) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		if err := r.RenderKumoData(ctx); err != nil {
			log.FromContext(ctx).Error(err, "rendering KumoMTA's data file")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// NeedLeaderElection implements manager.LeaderElectionRunnable: only the
// leader writes.
func (r *Renderer) NeedLeaderElection() bool { return true }
