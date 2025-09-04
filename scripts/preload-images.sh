#!/bin/bash
# Image pre-loading script for OpenSearch test harness

set -euo pipefail

# Common OpenSearch versions used in tests
OPENSEARCH_IMAGES=(
    "opensearchproject/opensearch:2.9.0"
    "opensearchproject/opensearch:3.0.0"
    "opensearchproject/opensearch:2.17.0"
    "opensearchproject/opensearch-dashboards:2.9.0"
    "opensearchproject/opensearch-dashboards:3.0.0"
    "opensearchproject/opensearch-dashboards:2.17.0"
)

# Utility images
UTILITY_IMAGES=(
    "busybox:latest"
    "curlimages/curl:latest"
    "nginx:alpine"
)

CLUSTER_NAME=${1:-"opensearch-upgrade-2.9-3.0"}

echo "🚀 Pre-loading images into Kind cluster: ${CLUSTER_NAME}"

# Check if cluster exists
if ! kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
    echo "❌ Kind cluster '${CLUSTER_NAME}' not found!"
    echo "Available clusters:"
    kind get clusters
    exit 1
fi

echo "📦 Pre-loading OpenSearch images..."
for image in "${OPENSEARCH_IMAGES[@]}"; do
    echo "  Loading: ${image}"
    
    # Pull image if not present locally
    if ! docker image inspect "${image}" >/dev/null 2>&1; then
        echo "    Pulling ${image}..."
        docker pull "${image}"
    else
        echo "    Image already cached locally ✓"
    fi
    
    # Load into Kind cluster
    kind load docker-image "${image}" --name "${CLUSTER_NAME}"
    echo "    Loaded into Kind cluster ✓"
done

echo "🛠️  Pre-loading utility images..."
for image in "${UTILITY_IMAGES[@]}"; do
    echo "  Loading: ${image}"
    
    if ! docker image inspect "${image}" >/dev/null 2>&1; then
        echo "    Pulling ${image}..."
        docker pull "${image}"
    else
        echo "    Image already cached locally ✓"
    fi
    
    kind load docker-image "${image}" --name "${CLUSTER_NAME}"
    echo "    Loaded into Kind cluster ✓"
done

echo ""
echo "✅ Image pre-loading complete!"
echo ""
echo "📊 Loaded images in cluster:"
docker exec "${CLUSTER_NAME}-worker" crictl images | grep -E "(opensearch|busybox|curl|nginx)" || echo "No matching images found"

echo ""
echo "🎯 Benefits:"
echo "  • Image pulls now use local cache (seconds instead of minutes)"
echo "  • Faster pod startup times"
echo "  • More reliable test execution"
echo "  • Reduced network dependency"