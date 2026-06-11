#!/bin/bash

# Define variables
UTILITY_DIR="../lakefusion_core_engine"          # Path to the lakefusion_core_engine project directory
SERVICE_DIR="$(pwd)"                             # Path to this service directory
WHEEL_DIR="$SERVICE_DIR/wheel"                   # Directory to store the wheel file
VERSION_FILE="../version.json"                   # Path to version.json in lakefusion_universe root

# Create directories if they don't exist
mkdir -p "$WHEEL_DIR"

# Navigate to the core engine project directory
cd "$UTILITY_DIR" || { echo "Core engine project directory not found!"; exit 1; }

# Clean previous build artifacts
echo "Cleaning previous build artifacts..."
rm -rf build dist *.egg-info "$WHEEL_DIR"/*.whl

# Generate the wheel file
echo "Generating wheel file for lakefusion_core_engine..."
python3 -m pip wheel . -w "$WHEEL_DIR" --no-cache-dir || { echo "Failed to generate wheel file!"; exit 1; }

# Verify wheel generation
WHEEL_NAME=$(ls "$WHEEL_DIR" | grep 'lakefusion_core_engine-.*-py3-none-any\.whl')

if [ -z "$WHEEL_NAME" ]; then
  echo "No wheel file found in $WHEEL_DIR"
  exit 1
fi

# Copy version.json to service directory (optional)
echo "Copying version.json to service directory..."
cp "$VERSION_FILE" "$SERVICE_DIR" || { echo "Failed to copy version.json!"; }

echo "Wheel file successfully generated at: $WHEEL_DIR/$WHEEL_NAME"
