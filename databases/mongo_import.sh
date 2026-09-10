#!/usr/bin/env bash
# Loads the 10,000 customer profile documents into MongoDB (spec 8.3).
#
# customerId is imported verbatim -- it is the join key back to nameOrig and
# nameDest in the transaction dataset, and any normalisation here would silently
# break the Alteryx blend.
set -euo pipefail

CONTAINER="${MONGO_CONTAINER:-finsight-mongodb}"
DB="${MONGO_DB:-novacrest}"
COLLECTION="${MONGO_COLLECTION:-customers}"
SRC="${MONGO_SRC:-/import/novacrest_customers.json}"
URI="mongodb://finsight:finsight@localhost:27017/?authSource=admin"

echo "==> importing $SRC into $DB.$COLLECTION"
docker exec "$CONTAINER" mongoimport \
  --uri "$URI" \
  --db "$DB" \
  --collection "$COLLECTION" \
  --file "$SRC" \
  --drop

echo
echo "==> applying indexes (spec 8.3 R1)"
docker exec -i "$CONTAINER" mongosh "$URI$DB" --quiet < "$(dirname "$0")/mongo_indexes.js"

echo
echo "==> validating import (spec 8.3 R2)"
docker exec -i "$CONTAINER" mongosh "$URI$DB" --quiet < "$(dirname "$0")/mongo_validate.js"
