// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package controller

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	postierv1 "github.com/SeeMyPing/postier/api/v1alpha1"
)

// ProjectReconciler mirrors Projects into the database.
//
// A deleted Project keeps its rows: its messages and suppressions still
// refer to them. What stops it from sending is the deletion of its
// credentials and domains, which have their own finalizers.
type ProjectReconciler struct {
	*Config
}

// Reconcile implements reconcile.Reconciler.
func (r *ProjectReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var project postierv1.Project
	if err := r.Client.Get(ctx, req.NamespacedName, &project); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !project.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	_, err := r.ensureProject(ctx, project.Namespace, project.Name)
	if err != nil {
		setCondition(&project.Status.Conditions, project.Generation, postierv1.ConditionReady,
			metav1.ConditionFalse, "SyncFailed", "could not write the project to the database")
		_ = r.Client.Status().Update(ctx, &project)
		return ctrl.Result{}, err
	}
	project.Status.ObservedGeneration = project.Generation
	setCondition(&project.Status.Conditions, project.Generation, postierv1.ConditionReady,
		metav1.ConditionTrue, "Synced", "the project exists in the data plane")
	return ctrl.Result{}, r.Client.Status().Update(ctx, &project)
}

// SetupWithManager registers the reconciler.
func (r *ProjectReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&postierv1.Project{}).Complete(r)
}
