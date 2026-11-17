// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package controller

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	postierv1 "github.com/SeeMyPing/postier/api/v1alpha1"
	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// ApiCredentialReconciler issues API keys.
//
// The key lives in a Secret next to the resource, for workloads to mount;
// the database only ever holds its hash. The Secret is the source of truth:
// whatever key it holds is the one that works. Deleting the Secret is
// therefore how a key is rotated, and deleting the resource how it is
// revoked.
type ApiCredentialReconciler struct { //nolint:revive // named after the kind
	*Config
}

// Reconcile implements reconcile.Reconciler.
func (r *ApiCredentialReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cred postierv1.ApiCredential
	if err := r.Client.Get(ctx, req.NamespacedName, &cred); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	externalID := string(cred.UID)

	if !cred.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cred, Finalizer) {
			// Revoke first, then let go: the key must be dead before the
			// resource disappears.
			if _, err := r.DB.Q().RevokeCredentialByExternalID(ctx, &externalID); err != nil {
				return ctrl.Result{}, fmt.Errorf("revoke: %w", err)
			}
			controllerutil.RemoveFinalizer(&cred, Finalizer)
			return ctrl.Result{}, r.Client.Update(ctx, &cred)
		}
		return ctrl.Result{}, nil
	}
	if controllerutil.AddFinalizer(&cred, Finalizer) {
		if err := r.Client.Update(ctx, &cred); err != nil {
			return ctrl.Result{}, err
		}
	}

	fail := func(reason, message string, requeue ctrl.Result, err error) (ctrl.Result, error) {
		setCondition(&cred.Status.Conditions, cred.Generation, postierv1.ConditionReady,
			metav1.ConditionFalse, reason, message)
		if uerr := r.Client.Status().Update(ctx, &cred); uerr != nil && err == nil {
			err = uerr
		}
		return requeue, err
	}

	ids, err := r.resolveProject(ctx, cred.Namespace, cred.Spec.ProjectRef.Name)
	if errors.Is(err, errProjectNotReady) {
		return fail("ProjectNotReady", fmt.Sprintf("project %q does not exist yet", cred.Spec.ProjectRef.Name),
			ctrl.Result{RequeueAfter: retryDependency}, nil)
	}
	if err != nil {
		return fail("SyncFailed", "could not read the project", ctrl.Result{}, err)
	}

	key, secretName, err := r.ensureSecret(ctx, &cred)
	if err != nil {
		var conflict *secretConflictError
		if errors.As(err, &conflict) {
			return fail("SecretConflict", conflict.Error(), ctrl.Result{}, nil)
		}
		return fail("SecretFailed", "could not write the Secret holding the key", ctrl.Result{}, err)
	}

	scopes := make([]string, len(cred.Spec.Scopes))
	for i, s := range cred.Spec.Scopes {
		scopes[i] = string(s)
	}
	err = r.DB.Q().UpsertCredentialByExternalID(ctx, sqlcgen.UpsertCredentialByExternalIDParams{
		ID: domain.NewID(), TenantID: ids.TenantID, ProjectID: ids.ProjectID,
		Name: cred.Namespace + "/" + cred.Name, Prefix: key.Prefix, SecretHash: key.SecretHash,
		Scopes: scopes, ExternalID: &externalID,
	})
	if err != nil {
		return fail("SyncFailed", "could not write the credential to the database", ctrl.Result{}, err)
	}

	cred.Status.ObservedGeneration = cred.Generation
	cred.Status.Prefix = key.Prefix
	cred.Status.SecretName = secretName
	setCondition(&cred.Status.Conditions, cred.Generation, postierv1.ConditionReady,
		metav1.ConditionTrue, "Issued", fmt.Sprintf("the key is in Secret %q", secretName))
	return ctrl.Result{}, r.Client.Status().Update(ctx, &cred)
}

// secretConflictError: a Secret of that name exists and is not ours.
type secretConflictError struct{ name string }

func (e *secretConflictError) Error() string {
	return fmt.Sprintf("Secret %q exists and does not belong to this credential", e.name)
}

// ensureSecret returns the key held by the credential's Secret, generating
// both when the Secret does not exist.
func (r *ApiCredentialReconciler) ensureSecret(ctx context.Context, cred *postierv1.ApiCredential) (accept.Key, string, error) {
	name := cred.Spec.SecretName
	if name == "" {
		name = cred.Name
	}
	var secret corev1.Secret
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: cred.Namespace, Name: name}, &secret)
	switch {
	case err == nil:
		// Never adopt a Secret someone else created: its content would
		// become a valid API key.
		if !metav1.IsControlledBy(&secret, cred) {
			return accept.Key{}, name, &secretConflictError{name: name}
		}
		token := string(secret.Data[postierv1.SecretKeyAPIKey])
		prefix, secretPart, perr := accept.ParseKey(token)
		if perr == nil {
			return accept.Key{Token: token, Prefix: prefix, SecretHash: accept.HashSecret(secretPart)}, name, nil
		}
		// Ours, but damaged: issue a new key in place.
		key, gerr := accept.GenerateKey(cred.Spec.Environment)
		if gerr != nil {
			return accept.Key{}, name, gerr
		}
		secret.Data = map[string][]byte{postierv1.SecretKeyAPIKey: []byte(key.Token)}
		return key, name, r.Client.Update(ctx, &secret)
	case apierrors.IsNotFound(err):
		key, gerr := accept.GenerateKey(cred.Spec.Environment)
		if gerr != nil {
			return accept.Key{}, name, gerr
		}
		secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: cred.Namespace, Name: name,
				Labels: map[string]string{"app.kubernetes.io/managed-by": "postier"},
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{postierv1.SecretKeyAPIKey: []byte(key.Token)},
		}
		// Owned by the credential: deleted with it by garbage collection.
		if err := controllerutil.SetControllerReference(cred, &secret, r.Client.Scheme()); err != nil {
			return accept.Key{}, name, err
		}
		err := r.Client.Create(ctx, &secret)
		if apierrors.IsAlreadyExists(err) {
			// It exists but is invisible to us: the operator only sees the
			// Secrets it labelled. So it is someone else's.
			return accept.Key{}, name, &secretConflictError{name: name}
		}
		return key, name, err
	default:
		return accept.Key{}, name, err
	}
}

// SetupWithManager registers the reconciler. It also watches the Secrets it
// owns: deleting one issues a new key.
func (r *ApiCredentialReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postierv1.ApiCredential{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
