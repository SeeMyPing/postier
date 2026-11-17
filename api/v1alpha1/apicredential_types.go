// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SecretKeyAPIKey is the key of the generated Secret that holds the API key.
const SecretKeyAPIKey = "api-key"

// ApiCredentialSpec describes an API key.
type ApiCredentialSpec struct {
	// ProjectRef is the project the key acts for.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="projectRef is immutable"
	ProjectRef ProjectReference `json:"projectRef"`

	// Scopes of the key. "admin" implies the others.
	// +kubebuilder:default={send,read}
	// +kubebuilder:validation:MinItems=1
	// +listType=set
	Scopes []Scope `json:"scopes,omitempty"`

	// Environment is the tag embedded in the key ("pst_live_…"), so that a
	// leaked key says what it opens.
	// +kubebuilder:default=live
	// +kubebuilder:validation:Enum=live;test
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="environment is immutable"
	Environment string `json:"environment,omitempty"`

	// SecretName is the Secret the key is written to, in the namespace of
	// the credential. Defaults to the name of the credential.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="secretName is immutable"
	SecretName string `json:"secretName,omitempty"`
}

// Scope is a permission of an API key.
// +kubebuilder:validation:Enum=send;read;suppressions;admin
type Scope string

// ApiCredentialStatus is the observed state of an ApiCredential.
type ApiCredentialStatus struct {
	// ObservedGeneration is the generation the conditions refer to.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Prefix is the public part of the key, as it appears in logs.
	// +optional
	Prefix string `json:"prefix,omitempty"`
	// SecretName is the Secret that holds the key.
	// +optional
	SecretName string `json:"secretName,omitempty"`
}

// ApiCredential issues an API key for a project. The operator generates the
// key, stores its hash in the database and writes the key itself to a Secret
// of the same namespace, for the workloads to mount. Deleting the resource
// revokes the key.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=apicredentials,singular=apicredential
// +kubebuilder:printcolumn:name="Project",type=string,JSONPath=`.spec.projectRef.name`
// +kubebuilder:printcolumn:name="Prefix",type=string,JSONPath=`.status.prefix`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ApiCredential struct { //nolint:revive // the kind is spelled ApiCredential in the architecture
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ApiCredentialSpec   `json:"spec"`
	Status ApiCredentialStatus `json:"status,omitempty"`
}

// ApiCredentialList is a list of ApiCredentials.
//
// +kubebuilder:object:root=true
type ApiCredentialList struct { //nolint:revive // see ApiCredential
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ApiCredential `json:"items"`
}
