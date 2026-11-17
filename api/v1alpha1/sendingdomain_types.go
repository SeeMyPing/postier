// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Condition types of a SendingDomain.
const (
	// ConditionOwnershipVerified: the challenge TXT record is published.
	ConditionOwnershipVerified = "OwnershipVerified"
	// ConditionDKIMPublished: the DKIM public key is published and matches.
	ConditionDKIMPublished = "DKIMPublished"
	// ConditionReturnPathValid: the return-path domain has its MX, and an
	// SPF record that covers every sending address.
	ConditionReturnPathValid = "ReturnPathValid"
	// ConditionDMARCPublished: a DMARC record exists. Informative: it does
	// not gate Ready, but Gmail and Yahoo require it from bulk senders.
	ConditionDMARCPublished = "DMARCPublished"
)

// DKIMSpec configures the DKIM signature of a domain.
type DKIMSpec struct {
	// Selector of the DKIM key. Immutable: changing the key of a domain is a
	// rotation, which publishes a new selector before using it.
	// +kubebuilder:default=pst1
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="selector is immutable"
	Selector string `json:"selector,omitempty"`
}

// ReturnPathSpec configures the envelope sender domain.
type ReturnPathSpec struct {
	// Subdomain of the sending domain that receives bounces. Its MX must
	// point at the platform.
	// +kubebuilder:default=bounce
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`
	Subdomain string `json:"subdomain,omitempty"`
}

// SendingDomainSpec describes a domain mail is sent from.
type SendingDomainSpec struct {
	// ProjectRef is the project allowed to send from this domain.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="projectRef is immutable"
	ProjectRef ProjectReference `json:"projectRef"`

	// Domain is the domain of the From address, in lowercase ASCII
	// (punycode for an internationalised name).
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="domain is immutable"
	Domain string `json:"domain"`

	// +optional
	// +kubebuilder:default={}
	DKIM DKIMSpec `json:"dkim,omitempty"`

	// +optional
	// +kubebuilder:default={}
	ReturnPath ReturnPathSpec `json:"returnPath,omitempty"`
}

// DNSRecord is a record the owner of the domain has to publish.
type DNSRecord struct {
	// Type of the record: TXT or MX.
	Type string `json:"type"`
	// Name of the record, fully qualified, without the trailing dot.
	Name string `json:"name"`
	// Value to publish.
	Value string `json:"value"`
	// Purpose says what the record is for.
	Purpose string `json:"purpose"`
}

// SendingDomainStatus is the observed state of a SendingDomain.
type SendingDomainStatus struct {
	// ObservedGeneration is the generation the conditions refer to.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// RequiredRecords lists the DNS records to publish for this domain.
	// +optional
	// +listType=atomic
	RequiredRecords []DNSRecord `json:"requiredRecords,omitempty"`
	// LastCheckTime is when DNS was last checked.
	// +optional
	LastCheckTime *metav1.Time `json:"lastCheckTime,omitempty"`
}

// SendingDomain declares a domain a project sends from. The operator
// generates its DKIM key, lists the DNS records to publish in the status,
// checks them, and only then lets the domain send.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Domain",type=string,JSONPath=`.spec.domain`
// +kubebuilder:printcolumn:name="Project",type=string,JSONPath=`.spec.projectRef.name`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type SendingDomain struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SendingDomainSpec   `json:"spec"`
	Status SendingDomainStatus `json:"status,omitempty"`
}

// SendingDomainList is a list of SendingDomains.
//
// +kubebuilder:object:root=true
type SendingDomainList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SendingDomain `json:"items"`
}
