#!/bin/bash
# Download CR-SQLite extension for benchmarking
#
# This script downloads the cr-sqlite extension needed for head-to-head
# benchmarks against DAG-CRR.

set -e

VENDOR_DIR="$(dirname "$0")/../vendor"
mkdir -p "$VENDOR_DIR"

# Detect platform
OS=$(uname -s)
ARCH=$(uname -m)

case "$OS" in
    Darwin)
        if [ "$ARCH" = "arm64" ]; then
            PLATFORM="darwin-aarch64"
            EXT="dylib"
        else
            PLATFORM="darwin-x86_64"
            EXT="dylib"
        fi
        ;;
    Linux)
        if [ "$ARCH" = "aarch64" ]; then
            PLATFORM="linux-aarch64"
            EXT="so"
        else
            PLATFORM="linux-x86_64"
            EXT="so"
        fi
        ;;
    *)
        echo "Unsupported platform: $OS"
        exit 1
        ;;
esac

# CR-SQLite release version (v0.16.3 is the latest stable as of Jan 2025)
VERSION="0.16.3"
FILENAME="crsqlite-${PLATFORM}.zip"
URL="https://github.com/vlcn-io/cr-sqlite/releases/download/v${VERSION}/${FILENAME}"

echo "Downloading CR-SQLite v${VERSION} for ${PLATFORM}..."
echo "URL: $URL"

cd "$VENDOR_DIR"

# Download
if command -v curl &> /dev/null; then
    curl -L -o "$FILENAME" "$URL"
elif command -v wget &> /dev/null; then
    wget -O "$FILENAME" "$URL"
else
    echo "Error: curl or wget required"
    exit 1
fi

# Extract
unzip -o "$FILENAME"
rm "$FILENAME"

# Rename to standard name
if [ -f "crsqlite.${EXT}" ]; then
    echo "CR-SQLite extension installed: vendor/crsqlite.${EXT}"
else
    # Try to find the extracted file
    FOUND=$(find . -name "*.${EXT}" -type f | head -1)
    if [ -n "$FOUND" ]; then
        mv "$FOUND" "crsqlite.${EXT}"
        echo "CR-SQLite extension installed: vendor/crsqlite.${EXT}"
    else
        echo "Warning: Could not find extracted extension"
        ls -la
    fi
fi

echo ""
echo "Setup complete. Run benchmarks with:"
echo "  cargo bench --bench crsqlite_comparison"
