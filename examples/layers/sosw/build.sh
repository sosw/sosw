#!/usr/bin/env bash
#
# Build the `sosw` AWS Lambda layer zip (python/ directory layout).
#
# By default installs the pinned release `sosw==3.0.0` from PyPI together with the
# default-on extras (aws-lambda-powertools, aws-xray-sdk). For pre-release builds
# use --use-local to install sosw from this repository checkout instead.
#
# Usage:
#   ./build.sh [--use-local] [--no-extras] [--sosw-version X.Y.Z] [--output FILE]
#
# Options:
#   --use-local           Install sosw from the repository this script lives in (pre-release builds).
#   --no-extras           Do not bundle aws-lambda-powertools and aws-xray-sdk.
#   --sosw-version X.Y.Z  Pin a different sosw release (default: 3.0.0). Ignored with --use-local.
#   --output FILE         Path of the resulting zip (default: ./sosw-layer.zip).
#
# Environment:
#   PYTHON                Python interpreter to build with (default: python3). Its pip is used,
#                         so pick the same minor version as your Lambda runtime when possible.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

PYTHON="${PYTHON:-python3}"
SOSW_VERSION='3.0.0'
USE_LOCAL='false'
WITH_EXTRAS='true'
OUTPUT="$(pwd)/sosw-layer.zip"

usage() {
    sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --use-local)    USE_LOCAL='true' ;;
        --no-extras)    WITH_EXTRAS='false' ;;
        --sosw-version) SOSW_VERSION="${2:?--sosw-version requires a value}"; shift ;;
        --output)       OUTPUT="${2:?--output requires a value}"; shift ;;
        -h|--help)      usage; exit 0 ;;
        *)              echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
    shift
done

case "${OUTPUT}" in
    /*) : ;;
    *)  OUTPUT="$(pwd)/${OUTPUT}" ;;
esac

BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/sosw-layer-build.XXXXXX")"
trap 'rm -rf "${BUILD_DIR}"' EXIT

if [ "${USE_LOCAL}" = 'true' ]; then
    SOSW_REQUIREMENT="${REPO_ROOT}"
    echo "Building sosw layer from local sources: ${REPO_ROOT}"
else
    SOSW_REQUIREMENT="sosw==${SOSW_VERSION}"
    echo "Building sosw layer from PyPI: ${SOSW_REQUIREMENT}"
fi

PACKAGES=("${SOSW_REQUIREMENT}")
if [ "${WITH_EXTRAS}" = 'true' ]; then
    PACKAGES+=('aws-lambda-powertools' 'aws-xray-sdk')
    echo "Including default extras: aws-lambda-powertools, aws-xray-sdk"
else
    echo "Skipping extras (--no-extras)"
fi

"${PYTHON}" -m pip install --quiet --target "${BUILD_DIR}/python" "${PACKAGES[@]}"

# Trim bytecode caches - dead weight in a layer.
find "${BUILD_DIR}/python" -type d -name '__pycache__' -prune -exec rm -rf {} +

mkdir -p "$(dirname "${OUTPUT}")"
rm -f "${OUTPUT}"
(cd "${BUILD_DIR}" && "${PYTHON}" -m zipfile -c "${OUTPUT}" python)

SIZE="$(du -h "${OUTPUT}" | cut -f1 | tr -d '[:space:]')"
echo "Layer zip built: ${OUTPUT} (${SIZE})"
echo "Publish it with: ./deploy.sh --zip ${OUTPUT}"
