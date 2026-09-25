#!/usr/bin/env bash
# ==============================================================================
# Snipr - Clear Elasticsearch Data
# ==============================================================================
set -euo pipefail

ELASTIC_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"

echo "=================================================="
echo "  SNIPR DATA RESET UTILITY"
echo "=================================================="

case "${1:-docs}" in
    docs)
        echo "Clearing all chat messages and clips from Elasticsearch..."
        curl -s -X POST "$ELASTIC_URL/snipr-*/_delete_by_query?conflicts=proceed" \
          -H "Content-Type: application/json" \
          -d '{"query": {"match_all": {}}}' > /dev/null
        echo "All documents in snipr-chat and snipr-clips cleared!"
        echo "   (Indices, mappings, and Kibana dashboards remain intact)"
        ;;

    indices)
        echo "Deleting snipr-chat and snipr-clips indices..."
        curl -s -X DELETE "$ELASTIC_URL/snipr-chat,snipr-clips" > /dev/null || true
        echo "Indices deleted. Snipr will auto-recreate them on boot."
        ;;

    all)
        echo "Resetting Docker containers and deleting storage volumes..."
        docker compose down -v
        echo "Starting fresh Elasticsearch and Kibana..."
        docker compose up -d
        echo "Complete reset complete. Run ./scripts/setup_kibana.sh to reload dashboards."
        ;;

    *)
        echo "Usage: ./scripts/clear_data.sh [docs|indices|all]"
        echo "  docs    -> (Default) Delete all documents from Elasticsearch (preserves mappings & dashboards)"
        echo "  indices -> Delete the snipr-chat and snipr-clips indices"
        echo "  all     -> Tear down Docker containers and wipe all storage volumes"
        exit 1
        ;;
esac

echo "=================================================="

