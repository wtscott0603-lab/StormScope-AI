#!/bin/sh
set -eu

TARGET_DIR="${1:-backend/api/static/geo}"
STATES_URL="https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json"
COUNTIES_URL="https://cdn.jsdelivr.net/gh/plotly/datasets@master/geojson-counties-fips.json"

mkdir -p "$TARGET_DIR"

curl -fsSL "$STATES_URL" -o "$TARGET_DIR/us_states.geojson"
curl -fsSL "$COUNTIES_URL" -o "$TARGET_DIR/us_counties.geojson"
