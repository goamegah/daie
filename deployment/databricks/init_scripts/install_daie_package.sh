#!/bin/bash
# Init script pour installer automatiquement le package daie
# Ce script s'exécute au démarrage du cluster

set -e

echo "🚀 Installing daie package..."

# Récupérer le tag Developer du cluster
DEVELOPER_TAG=$(echo "$DB_CLUSTER_CUSTOM_TAGS" | jq -r '.Developer // "dev"')

# Récupérer l'environnement depuis le nom du catalog
# Le catalog suit le pattern: daie_chn_{env}_bronze
CATALOG_PATTERN="daie_chn_*_bronze"
CATALOG=$(ls -d /Volumes/${CATALOG_PATTERN} 2>/dev/null | head -1 | xargs basename)

if [ -z "$CATALOG" ]; then
    echo "❌ ERROR: No catalog found matching pattern ${CATALOG_PATTERN}"
    exit 1
fi

# Chemins basés sur le développeur
VOLUME_PATH="/Volumes/${CATALOG}/artifacts/packages/${DEVELOPER_TAG}"
PACKAGE_NAME="daie"

echo "📋 Config:"
echo "   Developer: ${DEVELOPER_TAG}"
echo "   Catalog: ${CATALOG}"
echo "   Volume: ${VOLUME_PATH}"

# Trouver le dernier wheel
WHEEL_FILE=$(ls ${VOLUME_PATH}/${PACKAGE_NAME}-*.whl 2>/dev/null | sort -V | tail -1)

if [ -z "$WHEEL_FILE" ]; then
    echo "❌ ERROR: No wheel file found in ${VOLUME_PATH}"
    echo "   Make sure the package is deployed for developer: ${DEVELOPER_TAG}"
    exit 1
fi

echo "📦 Found wheel: ${WHEEL_FILE}"

# Installer le package
/databricks/python/bin/pip install --upgrade "${WHEEL_FILE}"

echo "✅ Package ${PACKAGE_NAME} installed successfully for ${DEVELOPER_TAG}!"