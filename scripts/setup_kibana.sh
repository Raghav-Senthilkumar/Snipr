#!/usr/bin/env bash
# ==============================================================================
# Snipr - Kibana Automated Dashboard & Data View Setup
# ==============================================================================
set -euo pipefail

KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
ELASTIC_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DASHBOARD_FILE="$ROOT_DIR/kibana/dashboard.ndjson"

echo "=================================================="
echo "  SNIPR KIBANA & ELASTICSEARCH AUTOMATION SETUP"
echo "=================================================="
echo "Kibana Target:        $KIBANA_URL"
echo "Elasticsearch Target: $ELASTIC_URL"
echo "Dashboard Definition: $DASHBOARD_FILE"
echo "--------------------------------------------------"

# 1. Wait for Kibana to become ready
echo "Checking Kibana availability at $KIBANA_URL..."
MAX_RETRIES=30
RETRY_COUNT=0

until curl -s -f "$KIBANA_URL/api/status" > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]; then
        echo "ERROR: Kibana did not become available at $KIBANA_URL after $((MAX_RETRIES * 2)) seconds."
        echo "   Make sure Docker is running: docker compose up -d"
        exit 1
    fi
    echo "   [Waiting] Kibana starting up... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

echo "Kibana is online and responsive!"

# 2. Create Data View: snipr-all (snipr-*)
echo "Setting up Data View: snipr-* (All Events)..."
curl -s -X POST "$KIBANA_URL/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "id": "snipr-all",
      "title": "snipr-*",
      "name": "Snipr All Events (Chat & Clips)",
      "timeFieldName": "@timestamp"
    }
  }' > /dev/null 2>&1 || true

# 3. Create Data View: snipr-chat*
echo "Setting up Data View: snipr-chat*..."
curl -s -X POST "$KIBANA_URL/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "id": "snipr-chat",
      "title": "snipr-chat*",
      "name": "Snipr Chat Stream",
      "timeFieldName": "@timestamp"
    }
  }' > /dev/null 2>&1 || true

# 4. Create Data View: snipr-clips*
echo "Setting up Data View: snipr-clips*..."
curl -s -X POST "$KIBANA_URL/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "id": "snipr-clips",
      "title": "snipr-clips*",
      "name": "Snipr Auto-Clips",
      "timeFieldName": "@timestamp"
    }
  }' > /dev/null 2>&1 || true

# 4. Import Dashboard & Visualizations (.ndjson)
if [ -f "$DASHBOARD_FILE" ]; then
    echo "Importing Dashboard & Lens Visualizations..."
    IMPORT_RESP=$(curl -s -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
      -H "kbn-xsrf: true" \
      --form file=@"$DASHBOARD_FILE")

    if echo "$IMPORT_RESP" | grep -q '"success":true'; then
        echo "Dashboard successfully imported!"
    else
        echo "INFO: Import response: $IMPORT_RESP"
    fi
else
    echo "WARN: Dashboard definition not found at $DASHBOARD_FILE"
fi

echo "=================================================="
echo "Setup Complete!"
echo "-> Open Kibana Dashboard:"
echo "   $KIBANA_URL/app/dashboards#/view/snipr-dashboard"
echo "-> Explore Raw Data Views in Discover:"
echo "   $KIBANA_URL/app/discover"
echo "=================================================="

