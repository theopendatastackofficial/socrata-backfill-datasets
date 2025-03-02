#!/bin/bash

# Step 1: Create the virtual environment
uv venv

# Step 2: Activate the virtual environment
if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate
else
    echo "Error: Virtual environment not found at .venv/bin/activate"
    exit 1
fi

# Step 3: Install dependencies
uv sync || { echo "Failed to sync dependencies"; exit 1; }

# Step 4: Ask user for SOCRATA_API_TOKEN
echo "Please enter your SOCRATA_API_TOKEN (press Enter to use the default community token):"
read -r SOCRATA_API_TOKEN
SOCRATA_API_TOKEN=${SOCRATA_API_TOKEN:-uHoP8dT0q1BTcacXLCcxrDp8z}

echo "Using SOCRATA_API_TOKEN: $SOCRATA_API_TOKEN"
if [ "$SOCRATA_API_TOKEN" == "uHoP8dT0q1BTcacXLCcxrDp8z" ]; then
    echo "Note: This is the default community token. Please use your own token if possible."
fi

# Step 5: Remove any old .env, then create new .env
rm -f .env
touch .env

# Step 6: Run exportpathlinux.py to retrieve LAKE_PATH (line1) and DAGSTER_HOME (line2)
readarray -t PATHS < <(uv run scripts/exportpathlinux.py)
LAKE_PATH="${PATHS[0]}"
DAGSTER_HOME="${PATHS[1]}"

if [ -z "$LAKE_PATH" ] || [ -z "$DAGSTER_HOME" ]; then
    echo "Error: Failed to retrieve LAKE_PATH or DAGSTER_HOME"
    exit 1
fi

# Step 7: Append env vars to .env
{
    echo "SOCRATA_API_TOKEN=$SOCRATA_API_TOKEN"
    echo "LAKE_PATH=$LAKE_PATH"
    echo "DAGSTER_HOME=$DAGSTER_HOME"
} >> .env

# Step 8: Generate dagster.yaml in DAGSTER_HOME
mkdir -p "$DAGSTER_HOME"
uv run scripts/generate_dagsteryaml.py "$DAGSTER_HOME" > "$DAGSTER_HOME/dagster.yaml"

# Step 9: Launch Dagster
echo "Starting Dagster development server..."
dagster dev
