#!/usr/bin/env bash

set -e # Script fails if any line fails

# Run Compilation
echo "Ensure you've revved the version number in package-node.json"
echo "Building relesae compiliation of MQL node library..."
rm -rf js/
shadow-cljs release library
cp package-node.json js/package.json


# Verify Functionality
echo "Running verification script for MQL NodeJS module..."
cp scripts/node_test.js js/node/
node js/node/node_test.js
echo "Successfully ran verification script."

# Release Library
if [[ $1 == "release" ]]; then
  echo "Releasing to NPM registry..."
  cd js
  npm publish
else
  echo ""
fi
rm -rf js/
