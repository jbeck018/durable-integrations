#!/usr/bin/env bash
# generate-zod.sh — Generates Zod validation schemas from the OpenAPI spec
# produced by buf generate. Run this after `buf generate` has produced the
# merged OpenAPI spec at gen/openapi/flowforge.swagger.json.
#
# Prerequisites:
#   npm install -g @hey-api/openapi-ts
#
# Output:
#   packages/proto-ts/src/schemas/  — Zod validators for all request/response types

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

OPENAPI_SPEC="$ROOT_DIR/gen/openapi/flowforge.swagger.json"
OUTPUT_DIR="$ROOT_DIR/packages/proto-ts/src/schemas"

if [ ! -f "$OPENAPI_SPEC" ]; then
  echo "Error: OpenAPI spec not found at $OPENAPI_SPEC"
  echo "Run 'buf generate' first to produce the OpenAPI spec."
  exit 1
fi

echo "Generating Zod schemas from $OPENAPI_SPEC..."

npx @hey-api/openapi-ts \
  -i "$OPENAPI_SPEC" \
  -o "$OUTPUT_DIR" \
  -c @hey-api/client-fetch \
  --plugins zod

echo "Zod schemas generated at $OUTPUT_DIR"
