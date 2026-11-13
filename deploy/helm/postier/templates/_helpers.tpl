{{/*
SPDX-FileCopyrightText: 2026 Nawer Kezal
SPDX-License-Identifier: AGPL-3.0-only
*/}}

{{- define "postier.fullname" -}}
{{- if contains "postier" .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-postier" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "postier.labels" -}}
app.kubernetes.io/name: postier
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* Selector labels of one component: {{ include "postier.selector" (list . "api") }} */}}
{{- define "postier.selector" -}}
{{- $root := index . 0 -}}
app.kubernetes.io/name: postier
app.kubernetes.io/instance: {{ $root.Release.Name }}
app.kubernetes.io/component: {{ index . 1 }}
{{- end -}}

{{- define "postier.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{- define "postier.secretName" -}}
{{- .Values.secrets.existingSecret | default (include "postier.fullname" .) -}}
{{- end -}}

{{/* Hardening shared by every Postier container (not KumoMTA, which starts
as root to drop privileges itself). */}}
{{- define "postier.securityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
capabilities:
  drop: ["ALL"]
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/* Environment variable read from the chart's Secret:
{{ include "postier.secretEnv" (list . "POSTIER_SIGNING_KEY" "signing-key") }} */}}
{{- define "postier.secretEnv" -}}
{{- $root := index . 0 -}}
- name: {{ index . 1 }}
  valueFrom:
    secretKeyRef:
      name: {{ include "postier.secretName" $root }}
      key: {{ index . 2 }}
{{- end -}}

{{- define "postier.probes" -}}
livenessProbe:
  httpGet:
    path: /healthz
    port: probes
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /readyz
    port: probes
  periodSeconds: 5
startupProbe:
  httpGet:
    path: /healthz
    port: probes
  periodSeconds: 2
  failureThreshold: 30
{{- end -}}

{{/* Listen address of KumoMTA's inbound SMTP: port 25 of the node in
hostNetwork mode, an unprivileged port behind a Service otherwise. */}}
{{- define "postier.kumoSmtpPort" -}}
{{- if .Values.kumomta.hostNetwork -}}25{{- else -}}2525{{- end -}}
{{- end -}}
