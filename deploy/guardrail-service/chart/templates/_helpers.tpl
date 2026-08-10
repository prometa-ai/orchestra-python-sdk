{{- define "prometa-guardrail.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "prometa-guardrail.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "prometa-guardrail.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "prometa-guardrail.selectorLabels" -}}
app.kubernetes.io/name: {{ include "prometa-guardrail.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "prometa-guardrail.labels" -}}
helm.sh/chart: {{ include "prometa-guardrail.chart" . }}
{{ include "prometa-guardrail.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: orchestra-tenant-runtime
{{- end -}}

{{- define "prometa-guardrail.image" -}}
{{- if .Values.image.digest -}}
{{- if not (regexMatch "^sha256:[a-f0-9]{64}$" .Values.image.digest) -}}
{{- fail "image.digest must be lowercase sha256:<64 hex>" -}}
{{- end -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
{{- end -}}

{{- define "prometa-guardrail.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "prometa-guardrail.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "prometa-guardrail.validateValues" -}}
{{- if and (empty .Values.guardrailConfig.existingSecret) (empty .Values.guardrailConfig.existingConfigMap) -}}
{{- fail "guardrailConfig.existingSecret or guardrailConfig.existingConfigMap is required; the chart never creates the profile document" -}}
{{- end -}}
{{- if and .Values.guardrailConfig.existingSecret .Values.guardrailConfig.existingConfigMap -}}
{{- fail "guardrailConfig.existingSecret and guardrailConfig.existingConfigMap are mutually exclusive" -}}
{{- end -}}
{{- if empty .Values.credentials.existingSecret -}}
{{- fail "credentials.existingSecret is required; the guardrail service refuses unauthenticated evaluate calls" -}}
{{- end -}}
{{- end -}}
