// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package controller holds the reconcilers of the Postier operator.
//
// The operator is the only Postier role that talks to the Kubernetes API.
// It turns custom resources into what the data plane reads: rows in
// Postgres, and the data file and DKIM keys of KumoMTA.
package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	postierv1 "github.com/SeeMyPing/postier/api/v1alpha1"
	"github.com/SeeMyPing/postier/internal/dnscheck"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// Finalizer delays the deletion of a resource until its effects are undone
// in the database: a deleted credential must stop working, a deleted domain
// must stop sending.
const Finalizer = "postier.dev/cleanup"

// Requeue intervals.
const (
	// retryDependency: a resource this one refers to is not there yet.
	retryDependency = 15 * time.Second
	// recheckPending: DNS of a domain that is not verified yet.
	recheckPending = 10 * time.Minute
	// recheckReady: DNS of a verified domain. Records do get removed.
	recheckReady = 6 * time.Hour
)

// Config is what every reconciler needs.
type Config struct {
	Client client.Client
	DB     *store.SystemPool
	Signer *domain.Signer
	DNS    dnscheck.Resolver
	// SkipDNS declares every domain verified without looking. For clusters
	// with no real DNS (the kind quickstart), never for production.
	SkipDNS bool
	// Namespace is where the operator runs, and where KumoMTA's data file
	// and DKIM keys live.
	Namespace string
	// BaseConfigMap holds the part of KumoMTA's data file that comes from
	// the installation (key base.json). DataConfigMap receives the complete
	// file (key data.json). DKIMSecret receives the private keys.
	BaseConfigMap string
	DataConfigMap string
	DKIMSecret    string
	// DKIMKeyDir is where KumoMTA finds the keys of DKIMSecret.
	DKIMKeyDir string
	// KumoPodLabels selects KumoMTA's pods in Namespace, to make them pick a
	// new data file up at once. Empty: they pick it up at the kubelet's pace.
	KumoPodLabels map[string]string
	Now           func() time.Time
}

func (c *Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// setCondition records a condition for the current generation.
func setCondition(conds *[]metav1.Condition, generation int64, typ string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: message, ObservedGeneration: generation,
	})
}

func boolStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// projectIDs resolves a Project of a namespace to its database rows. The
// tenant is the namespace, until the Tenant resource exists.
type projectIDs struct {
	TenantID  uuid.UUID
	ProjectID uuid.UUID
}

var errProjectNotReady = errors.New("project not found or not ready")

// resolveProject returns the rows of a Project that has been reconciled.
func (c *Config) resolveProject(ctx context.Context, namespace, name string) (projectIDs, error) {
	var project postierv1.Project
	if err := c.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &project); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return projectIDs{}, errProjectNotReady
		}
		return projectIDs{}, err
	}
	if !meta.IsStatusConditionTrue(project.Status.Conditions, postierv1.ConditionReady) {
		return projectIDs{}, errProjectNotReady
	}
	return c.ensureProject(ctx, namespace, name)
}

// ensureProject creates the tenant and project rows if needed. Both upserts
// are idempotent and return the existing id.
func (c *Config) ensureProject(ctx context.Context, namespace, name string) (projectIDs, error) {
	var ids projectIDs
	err := c.DB.InTx(ctx, func(q *store.Queries, _ pgx.Tx) error {
		var err error
		ids.TenantID, err = q.UpsertTenant(ctx, sqlcgen.UpsertTenantParams{ID: domain.NewID(), Name: namespace})
		if err != nil {
			return fmt.Errorf("tenant: %w", err)
		}
		ids.ProjectID, err = q.UpsertProject(ctx, sqlcgen.UpsertProjectParams{
			ID: domain.NewID(), TenantID: ids.TenantID, Name: name,
		})
		if err != nil {
			return fmt.Errorf("project: %w", err)
		}
		return nil
	})
	return ids, err
}
