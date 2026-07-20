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
{{- if and (not (empty .Values.runtimeConfig.rolloutId)) (not (regexMatch "^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,199}$" .Values.runtimeConfig.rolloutId)) -}}
{{- fail "runtimeConfig.rolloutId must be a bounded deployment identifier" -}}
{{- end -}}
{{- if hasKey .Values.podAnnotations "prometa.io/runtime-config-rollout-id" -}}
{{- fail "podAnnotations cannot override prometa.io/runtime-config-rollout-id" -}}
{{- end -}}
{{- if hasKey .Values.podAnnotations "prometa.io/production-profile-id" -}}
{{- fail "podAnnotations cannot override prometa.io/production-profile-id" -}}
{{- end -}}
{{- if or (empty .Values.credentials.databaseUrlKey) (empty .Values.credentials.apiTokenKey) (empty .Values.credentials.modelGatewayApiKeyKey) (empty .Values.credentials.controlPlaneApiKeyKey) (empty .Values.credentials.receiptApiKeyKey) -}}
{{- fail "all credentials key names must be non-empty" -}}
{{- end -}}
{{- if and (or .Values.migration.enabled .Values.migration.compatibilityCheck) (empty .Values.migration.serviceAccountName) -}}
{{- fail "migration.serviceAccountName must name a pre-existing account" -}}
{{- end -}}
{{- if and (or .Values.migration.enabled .Values.migration.compatibilityCheck) .Values.migration.networkPolicy.enabled (empty .Values.migration.networkPolicy.egress) -}}
{{- fail "migration.networkPolicy.egress is required while its fail-closed policy is enabled" -}}
{{- end -}}
{{- if .Values.backup.enabled -}}
{{- if not .Values.backup.acknowledgeSensitiveData -}}
{{- fail "backup.acknowledgeSensitiveData must be true because runtime backups contain sensitive release and receipt data" -}}
{{- end -}}
{{- if or (empty .Values.backup.existingClaim) (empty .Values.backup.existingSecret) (empty .Values.backup.schedule) -}}
{{- fail "enabled backup requires existingClaim, existingSecret, and schedule" -}}
{{- end -}}
{{- if or (empty .Values.backup.hostKey) (empty .Values.backup.portKey) (empty .Values.backup.databaseKey) (empty .Values.backup.usernameKey) (empty .Values.backup.passwordKey) -}}
{{- fail "all backup database credential key names must be non-empty" -}}
{{- end -}}
{{- if or (lt (int .Values.backup.retentionDays) 1) (gt (int .Values.backup.retentionDays) 3650) -}}
{{- fail "backup.retentionDays must be between 1 and 3650" -}}
{{- end -}}
{{- if not (regexMatch "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$" .Values.backup.filenamePrefix) -}}
{{- fail "backup.filenamePrefix must be a safe 1-64 character basename prefix" -}}
{{- end -}}
{{- if and .Values.backup.networkPolicy.enabled (empty .Values.backup.networkPolicy.egress) -}}
{{- fail "backup.networkPolicy.egress is required while its fail-closed policy is enabled" -}}
{{- end -}}
{{- end -}}
{{- if and .Values.networkPolicy.enabled (empty .Values.networkPolicy.egress) -}}
{{- fail "networkPolicy.egress must explicitly allow the runtime database and model gateway" -}}
{{- end -}}
{{- if and (not (empty .Values.runtimeEdge.overloadContract)) (ne .Values.runtimeEdge.overloadContract "orchestra-runtime-edge-overload-v1") -}}
{{- fail "runtimeEdge.overloadContract is unsupported" -}}
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
{{- if .Values.productionProfile.enabled -}}
{{- $profileId := required "productionProfile.profileId is required" .Values.productionProfile.profileId -}}
{{- if ne .Values.runtimeEdge.overloadContract "orchestra-runtime-edge-overload-v1" -}}
{{- fail "the OpenShift runtime profile requires orchestra-runtime-edge-overload-v1" -}}
{{- end -}}
{{- if ne .Values.productionProfile.imageFlavor "ubi9" -}}
{{- fail "productionProfile.imageFlavor must be ubi9" -}}
{{- end -}}
{{- if not .Values.productionProfile.namespaceDefaultDenyAcknowledged -}}
{{- fail "the OpenShift runtime profile requires acknowledgement of a pre-created namespace-wide default-deny policy" -}}
{{- end -}}
{{- if or (empty .Values.image.repository) (empty .Values.image.digest) -}}
{{- fail "the OpenShift runtime profile requires an immutable image repository and digest" -}}
{{- end -}}
{{- if or (empty .Values.runtimeConfig.existingSecret) (not (empty .Values.runtimeConfig.existingConfigMap)) (empty .Values.runtimeConfig.rolloutId) -}}
{{- fail "the OpenShift runtime profile requires an immutable Secret-backed runtime config and rolloutId" -}}
{{- end -}}
{{- if or (empty .Values.migration.existingSecret) (eq .Values.migration.existingSecret .Values.credentials.existingSecret) -}}
{{- fail "the OpenShift runtime profile requires a separate migration credential Secret" -}}
{{- end -}}
{{- if or (not .Values.migration.enabled) (not .Values.migration.compatibilityCheck) (eq .Values.migration.serviceAccountName "default") -}}
{{- fail "the OpenShift runtime profile requires migration, compatibility checking, and a dedicated pre-created migration ServiceAccount" -}}
{{- end -}}
{{- if or (not .Values.migration.networkPolicy.enabled) (empty .Values.migration.networkPolicy.egress) -}}
{{- fail "the OpenShift runtime profile requires scoped migration NetworkPolicy egress" -}}
{{- end -}}
{{- if or (not .Values.networkPolicy.enabled) (empty .Values.networkPolicy.ingress) (empty .Values.networkPolicy.egress) -}}
{{- fail "the OpenShift runtime profile requires explicit runtime ingress and egress policies" -}}
{{- end -}}
{{- if .Values.autoscaling.enabled -}}
{{- if lt (int .Values.autoscaling.minReplicas) 2 -}}
{{- fail "the OpenShift runtime profile requires autoscaling.minReplicas >= 2" -}}
{{- end -}}
{{- else if lt (int .Values.replicaCount) 2 -}}
{{- fail "the OpenShift runtime profile requires replicaCount >= 2" -}}
{{- end -}}
{{- if not .Values.podDisruptionBudget.enabled -}}
{{- fail "the OpenShift runtime profile requires a PodDisruptionBudget" -}}
{{- end -}}
{{- if empty .Values.topologySpreadConstraints -}}
{{- fail "the OpenShift runtime profile requires topology spread constraints" -}}
{{- end -}}
{{- if .Values.backup.enabled -}}
{{- fail "the OpenShift runtime profile delegates PostgreSQL backup and restore to the external database operator" -}}
{{- end -}}
{{- if ne .Values.service.type "ClusterIP" -}}
{{- fail "the OpenShift runtime profile exposes only an internal ClusterIP service to the tenant gateway" -}}
{{- end -}}
{{- if .Values.serviceAccount.automountServiceAccountToken -}}
{{- fail "the OpenShift runtime profile forbids service-account token automount" -}}
{{- end -}}
{{- if or (hasKey .Values.podSecurityContext "runAsUser") (hasKey .Values.podSecurityContext "runAsGroup") (hasKey .Values.podSecurityContext "fsGroup") -}}
{{- fail "the OpenShift runtime profile delegates UID/GID allocation to restricted-v2" -}}
{{- end -}}
{{- if or (not .Values.podSecurityContext.runAsNonRoot) (ne .Values.podSecurityContext.seccompProfile.type "RuntimeDefault") -}}
{{- fail "the OpenShift runtime profile requires runAsNonRoot and RuntimeDefault seccomp" -}}
{{- end -}}
{{- if or .Values.containerSecurityContext.allowPrivilegeEscalation (not .Values.containerSecurityContext.readOnlyRootFilesystem) (not (has "ALL" .Values.containerSecurityContext.capabilities.drop)) -}}
{{- fail "the OpenShift runtime profile requires read-only root, no privilege escalation, and dropped capabilities" -}}
{{- end -}}
{{- if or .Values.credentials.modelGatewayApiKeyOptional .Values.credentials.receiptApiKeyOptional -}}
{{- fail "the OpenShift runtime profile requires model-gateway and asynchronous receipt credentials" -}}
{{- end -}}
{{- end -}}
{{- range .Values.extraEnv -}}
{{- if has .name (list "PORT" "PROMETA_RUNTIME_HOST" "PROMETA_RUNTIME_CONFIG" "PROMETA_RUNTIME_DATABASE_URL" "PROMETA_RUNTIME_API_TOKEN" "PROMETA_RUNTIME_EDGE_OVERLOAD_CONTRACT" "MODEL_GATEWAY_API_KEY" "ORCHESTRA_RUNTIME_CONTROL_PLANE_API_KEY" "ORCHESTRA_RUNTIME_RECEIPT_API_KEY") -}}
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
