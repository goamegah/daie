#!/bin/bash
# Script pour configurer manuellement l'allowlist des init scripts
# Usage: ./configure_allowlist_manual.sh <environment>

set -e

if [ $# -lt 1 ]; then
    echo "Usage: ./configure_allowlist_manual.sh <environment>"
    echo ""
    echo "Examples:"
    echo "  ./configure_allowlist_manual.sh dev"
    echo "  ./configure_allowlist_manual.sh test"
    exit 1
fi

ENV=$1
CATALOG="daie_chn_${ENV}_bronze"
PATTERN="/Volumes/${CATALOG}/artifacts/init_scripts/*"

echo "============================================================"
echo "Configuring Init Script Allowlist"
echo "Environment: ${ENV}"
echo "Pattern: ${PATTERN}"
echo "============================================================"
echo ""

# Vérifier les variables d'environnement
if [ -z "$DATABRICKS_HOST" ] || [ -z "$AZURE_CLIENT_ID" ] || [ -z "$AZURE_CLIENT_SECRET" ] || [ -z "$AZURE_TENANT_ID" ]; then
    echo "❌ Missing environment variables:"
    echo "   DATABRICKS_HOST"
    echo "   AZURE_CLIENT_ID"
    echo "   AZURE_CLIENT_SECRET"
    echo "   AZURE_TENANT_ID"
    exit 1
fi

# Obtenir un token d'accès
echo "🔐 Getting access token..."
TOKEN_RESPONSE=$(curl -s -X POST "https://login.microsoftonline.com/${AZURE_TENANT_ID}/oauth2/v2.0/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "client_id=${AZURE_CLIENT_ID}" \
    -d "client_secret=${AZURE_CLIENT_SECRET}" \
    -d "grant_type=client_credentials" \
    -d "scope=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default")

ACCESS_TOKEN=$(echo $TOKEN_RESPONSE | jq -r '.access_token')

if [ "$ACCESS_TOKEN" == "null" ] || [ -z "$ACCESS_TOKEN" ]; then
    echo "❌ Failed to get access token"
    echo "$TOKEN_RESPONSE"
    exit 1
fi

echo "✅ Token obtained"
echo ""

# Configurer l'allowlist
echo "🔧 Configuring allowlist..."
RESPONSE=$(curl -s -X PATCH "${DATABRICKS_HOST}/api/2.0/workspace-conf" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{
        \"enableInitScriptAllowlist\": \"true\",
        \"initScriptAllowlist\": \"${PATTERN}\"
    }")

if echo "$RESPONSE" | jq -e '.error_code' > /dev/null 2>&1; then
    echo "❌ Error configuring allowlist:"
    echo "$RESPONSE" | jq '.'
    echo ""
    echo "⚠️  You may need admin permissions"
    echo ""
    echo "📝 Manual configuration:"
    echo "   1. Go to Databricks Admin Console"
    echo "   2. Workspace Settings > Advanced"
    echo "   3. Enable 'Init script allowlist'"
    echo "   4. Add pattern: ${PATTERN}"
    exit 1
fi

echo "✅ Allowlist configured successfully"
echo ""
echo "💡 All init scripts in ${PATTERN} are now allowed"
