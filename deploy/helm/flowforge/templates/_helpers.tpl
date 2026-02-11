{{/*
Expand the name of the chart.
*/}}
{{- define "flowforge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated at 63 chars because some Kubernetes name fields are limited to this.
*/}}
{{- define "flowforge.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "flowforge.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "flowforge.labels" -}}
helm.sh/chart: {{ include "flowforge.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{ include "flowforge.selectorLabels" . }}
{{- end }}

{{/*
Selector labels used for matching pods to services/deployments.
*/}}
{{- define "flowforge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flowforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Component-specific labels. Pass a dict with keys "context" (the root context)
and "component" (a string like "api", "worker", "mcp-gateway").
*/}}
{{- define "flowforge.componentLabels" -}}
{{ include "flowforge.labels" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Component-specific selector labels.
*/}}
{{- define "flowforge.componentSelectorLabels" -}}
{{ include "flowforge.selectorLabels" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "flowforge.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "flowforge.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image reference helper. Pass a dict with "registry" (global) and "image" (component image config).
*/}}
{{- define "flowforge.image" -}}
{{- if .registry }}
{{- printf "%s/%s:%s" .registry .image.repository (.image.tag | default "latest") }}
{{- else }}
{{- printf "%s:%s" .image.repository (.image.tag | default "latest") }}
{{- end }}
{{- end }}

{{/*
PostgreSQL host -- internal service name or external override.
*/}}
{{- define "flowforge.postgresHost" -}}
{{- if .Values.postgresql.internal }}
{{- printf "%s-postgresql" (include "flowforge.fullname" .) }}
{{- else }}
{{- .Values.postgresql.host }}
{{- end }}
{{- end }}

{{/*
Redis host -- internal service name or external override.
*/}}
{{- define "flowforge.redisHost" -}}
{{- if .Values.redis.internal }}
{{- printf "%s-redis" (include "flowforge.fullname" .) }}
{{- else }}
{{- .Values.redis.host }}
{{- end }}
{{- end }}
