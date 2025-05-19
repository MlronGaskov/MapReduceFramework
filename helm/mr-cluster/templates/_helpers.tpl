{{/* ----------------------------------------------------------------------------
Define chart-wide helper templates for mr-cluster
----------------------------------------------------------------------------- */}}

{{/*
Return the name of the chart, allowing an override
*/}}
{{- define "mr-cluster.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Return the full name: <release-name>-<chart-name>
*/}}
{{- define "mr-cluster.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "mr-cluster.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels to apply to all resources
*/}}
{{- define "mr-cluster.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/name: {{ include "mr-cluster.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Helper to include common labels under metadata.labels
*/}}
{{- define "mr-cluster.metadata" -}}
labels:
  {{ include "mr-cluster.labels" . | indent 2 }}
{{- end -}}
