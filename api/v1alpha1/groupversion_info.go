// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package v1alpha1 holds the Postier custom resources.
//
// Resources describe configuration: who may send, from which domains, with
// which credentials. The operator mirrors them into Postgres, which is what
// the data plane reads. Messages, events and suppressions never go through
// the Kubernetes API.
//
// +kubebuilder:object:generate=true
// +groupName=postier.dev
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group and version of these resources.
	GroupVersion = schema.GroupVersion{Group: "postier.dev", Version: "v1alpha1"}

	// SchemeBuilder registers the types with a scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes lists every kind of the group. This package only depends on
// apimachinery, so that clients can import it cheaply.
func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion,
		&Project{}, &ProjectList{},
		&SendingDomain{}, &SendingDomainList{},
		&ApiCredential{}, &ApiCredentialList{},
	)
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}

// Condition types shared by the resources.
const (
	// ConditionReady is true when the resource is in effect in the data plane.
	ConditionReady = "Ready"
)

// ProjectReference names a Project of the same namespace.
type ProjectReference struct {
	// Name of the Project.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}
