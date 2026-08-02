#!/usr/bin/env bash
#
# Publish the `sosw` Lambda layer zip and point the SSM parameter at the new version.
#
# Publishes a new layer version with `aws lambda publish-layer-version` and updates the
# SSM parameter (default `lambda-layer-sosw-latest`) that SAM templates consume via
# `AWS::SSM::Parameter::Value<String>`. Idempotent: when the latest published layer
# version already has the same content hash as the local zip, publishing is skipped and
# only the SSM pointer is reconciled.
#
# Usage:
#   ./deploy.sh [--zip FILE] [--layer-name NAME] [--ssm-parameter NAME]
#               [--region REGION] [--profile PROFILE] [--dry-run]
#
# Options:
#   --zip FILE            Layer zip built by build.sh (default: ./sosw-layer.zip).
#   --layer-name NAME     Lambda layer name (default: sosw).
#   --ssm-parameter NAME  SSM parameter holding the latest layer ARN (default: lambda-layer-sosw-latest).
#   --region REGION       AWS region (default: from AWS_REGION/AWS_DEFAULT_REGION or the profile).
#   --profile PROFILE     AWS CLI profile (default: from AWS_PROFILE or the default credentials chain).
#   --dry-run             Print the AWS commands without executing anything.

set -euo pipefail

ZIP_FILE='sosw-layer.zip'
LAYER_NAME='sosw'
SSM_PARAMETER='lambda-layer-sosw-latest'
DRY_RUN='false'
AWS_ARGS=()

COMPATIBLE_RUNTIMES='python3.12 python3.13 python3.14'

usage() {
    sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --zip)           ZIP_FILE="${2:?--zip requires a value}"; shift ;;
        --layer-name)    LAYER_NAME="${2:?--layer-name requires a value}"; shift ;;
        --ssm-parameter) SSM_PARAMETER="${2:?--ssm-parameter requires a value}"; shift ;;
        --region)        AWS_ARGS+=('--region' "${2:?--region requires a value}"); shift ;;
        --profile)       AWS_ARGS+=('--profile' "${2:?--profile requires a value}"); shift ;;
        --dry-run)       DRY_RUN='true' ;;
        -h|--help)       usage; exit 0 ;;
        *)               echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
    shift
done

# Portable expansion of a possibly-empty array under `set -u` (bash 3.2 compatible).
aws_cli() {
    aws ${AWS_ARGS[@]+"${AWS_ARGS[@]}"} "$@"
}

DESCRIPTION="sosw framework layer built from $(basename "${ZIP_FILE}") on $(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [ "${DRY_RUN}" = 'true' ]; then
    echo "[dry-run] Would run:"
    echo "[dry-run]   aws ${AWS_ARGS[*]-} lambda publish-layer-version --layer-name ${LAYER_NAME} \\"
    echo "[dry-run]       --description '${DESCRIPTION}' --license-info MIT \\"
    echo "[dry-run]       --zip-file fileb://${ZIP_FILE} --compatible-runtimes ${COMPATIBLE_RUNTIMES}"
    echo "[dry-run]   aws ${AWS_ARGS[*]-} ssm put-parameter --name ${SSM_PARAMETER} --type String \\"
    echo "[dry-run]       --value <LayerVersionArn> --overwrite"
    echo "[dry-run] (publishing is skipped when the latest layer version already has the same CodeSha256)"
    exit 0
fi

if [ ! -f "${ZIP_FILE}" ]; then
    echo "Layer zip not found: ${ZIP_FILE}. Build it first: ./build.sh --output ${ZIP_FILE}" >&2
    exit 1
fi

LOCAL_SHA="$(openssl dgst -sha256 -binary "${ZIP_FILE}" | base64)"

LATEST_VERSION="$(aws_cli lambda list-layer-versions --layer-name "${LAYER_NAME}" --max-items 1 \
                  --query 'LayerVersions[0].Version' --output text)"

LAYER_ARN=''
if [ "${LATEST_VERSION}" != 'None' ] && [ -n "${LATEST_VERSION}" ]; then
    REMOTE_SHA="$(aws_cli lambda get-layer-version --layer-name "${LAYER_NAME}" \
                  --version-number "${LATEST_VERSION}" --query 'Content.CodeSha256' --output text)"

    if [ "${REMOTE_SHA}" = "${LOCAL_SHA}" ]; then
        LAYER_ARN="$(aws_cli lambda get-layer-version --layer-name "${LAYER_NAME}" \
                     --version-number "${LATEST_VERSION}" --query 'LayerVersionArn' --output text)"
        echo "Layer version ${LATEST_VERSION} already has this content (CodeSha256 match). Skipping publish."
    fi
fi

if [ -z "${LAYER_ARN}" ]; then
    # shellcheck disable=SC2086  # COMPATIBLE_RUNTIMES is intentionally word-split.
    LAYER_ARN="$(aws_cli lambda publish-layer-version \
                 --layer-name "${LAYER_NAME}" \
                 --description "${DESCRIPTION}" \
                 --license-info 'MIT' \
                 --zip-file "fileb://${ZIP_FILE}" \
                 --compatible-runtimes ${COMPATIBLE_RUNTIMES} \
                 --query 'LayerVersionArn' --output text)"
    echo "Published new layer version: ${LAYER_ARN}"
fi

CURRENT_POINTER="$(aws_cli ssm get-parameter --name "${SSM_PARAMETER}" \
                   --query 'Parameter.Value' --output text 2>/dev/null || echo '')"

if [ "${CURRENT_POINTER}" = "${LAYER_ARN}" ]; then
    echo "SSM parameter ${SSM_PARAMETER} already points at ${LAYER_ARN}. Nothing to do."
else
    aws_cli ssm put-parameter --name "${SSM_PARAMETER}" --type 'String' \
        --value "${LAYER_ARN}" --overwrite > /dev/null
    echo "SSM parameter ${SSM_PARAMETER} updated: ${LAYER_ARN}"
fi
