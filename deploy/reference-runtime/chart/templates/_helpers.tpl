{{- define "prometa-runtime.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "prometa-runtime.fullname" -}}
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

{{- define "prometa-runtime.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "prometa-runtime.selectorLabels" -}}
app.kubernetes.io/name: {{ include "prometa-runtime.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "prometa-runtime.labels" -}}
helm.sh/chart: {{ include "prometa-runtime.chart" . }}
{{ include "prometa-runtime.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: orchestra-tenant-runtime
{{- end -}}

{{- define "prometa-runtime.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{- define "prometa-runtime.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "prometa-runtime.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "prometa-runtime.migrationSecretName" -}}
{{- default .Values.credentials.existingSecret .Values.migration.existingSecret -}}
{{- end -}}

{{- define "prometa-runtime.validateValues" -}}
{{- if and (empty .Values.runtimeConfig.existingSecret) (empty .Values.runtimeConfig.existingConfigMap) -}}
{{- fail "set exactly one of runtimeConfig.existingSecret or runtimeConfig.existingConfigMap" -}}
{{- end -}}
{{- if and (not (empty .Values.runtimeConfig.existingSecret)) (not (empty .Values.runtimeConfig.existingConfigMap)) -}}
{{- fail "runtimeConfig.existingSecret and runtimeConfig.existingConfigMap are mutually exclusive" -}}
{{- end -}}
{{- if empty .Values.credentials.existingSecret -}}
{{- fail "credentials.existingSecret is required; the chart never creates credentials" -}}
{{- end -}}
{{- if or (empty .Values.runtimeConfig.key) (not (regexMatch "^[A-Za-z0-9._-]+$" .Values.runtimeConfig.targetFile)) -}}
{{- fail "runtimeConfig.key and a basename-only runtimeConfig.targetFile are required" -}}
{{- end -}}
{{- if or (empty .Values.credentials.databaseUrlKey) (empty .Values.credentials.apiTokenKey) (empty .Values.credentials.modelGatewayApiKeyKey) -}}
{{- fail "all credentials key names must be non-empty" -}}
{{- end -}}
{{- if and .Values.migration.enabled (empty .Values.migration.serviceAccountName) -}}
{{- fail "migration.serviceAccountName must name a pre-existing account" -}}
{{- end -}}
{{- if and .Values.migration.enabled .Values.migration.networkPolicy.enabled (empty .Values.migration.networkPolicy.egress) -}}
{{- fail "migration.networkPolicy.egress is required while its fail-closed policy is enabled" -}}
{{- end -}}
{{- if and .Values.networkPolicy.enabled (empty .Values.networkPolicy.egress) -}}
{{- fail "networkPolicy.egress must explicitly allow the runtime database and model gateway" -}}
{{- end -}}
{{- if and .Values.autoscaling.enabled (empty .Values.autoscaling.targetCPUUtilizationPercentage) (empty .Values.autoscaling.targetMemoryUtilizationPercentage) -}}
{{- fail "autoscaling requires at least one CPU or memory utilization target" -}}
{{- end -}}
{{- if and .Values.autoscaling.enabled (gt (int .Values.autoscaling.minReplicas) (int .Values.autoscaling.maxReplicas)) -}}
{{- fail "autoscaling.minReplicas cannot exceed autoscaling.maxReplicas" -}}
{{- end -}}
{{- if and .Values.podDisruptionBudget.enabled (not (empty .Values.podDisruptionBudget.minAvailable)) (not (empty .Values.podDisruptionBudget.maxUnavailable)) -}}
{{- fail "set only one of podDisruptionBudget.minAvailable or maxUnavailable" -}}
{{- end -}}
{{- if le (int .Values.gracefulShutdown.terminationGracePeriodSeconds) (int .Values.gracefulShutdown.preStopSleepSeconds) -}}
{{- fail "terminationGracePeriodSeconds must exceed preStopSleepSeconds" -}}
{{- end -}}
{{- range .Values.extraEnv -}}
{{- if has .name (list "PORT" "PROMETA_RUNTIME_HOST" "PROMETA_RUNTIME_CONFIG" "PROMETA_RUNTIME_DATABASE_URL" "PROMETA_RUNTIME_API_TOKEN" "MODEL_GATEWAY_API_KEY") -}}
{{- fail (printf "extraEnv cannot override reserved variable %s" .name) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "prometa-runtime.dnsEgress" -}}
- to:
    - namespaceSelector:
        {{- toYaml .dnsNamespaceSelector | nindent 8 }}
  ports:
    - protocol: UDP
      port: 53
    - protocol: TCP
      port: 53
{{- end -}}
