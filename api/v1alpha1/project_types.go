// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ProjectSpec describes a project.
type ProjectSpec struct {
	// DisplayName is a human-readable name, for consoles and reports.
	// +optional
	// +kubebuilder:validation:MaxLength=120
	DisplayName string `json:"displayName,omitempty"`
}

// ProjectStatus is the observed state of a Project.
type ProjectStatus struct {
	// ObservedGeneration is the generation the conditions refer to.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Project groups the sending domains, credentials and suppressions of one
// application or environment. Until the Tenant resource exists, the
// namespace of a Project is its tenant.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Project struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProjectSpec   `json:"spec,omitempty"`
	Status ProjectStatus `json:"status,omitempty"`
}

// ProjectList is a list of Projects.
//
// +kubebuilder:object:root=true
type ProjectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Project `json:"items"`
}
