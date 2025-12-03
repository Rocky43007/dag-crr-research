#!/usr/bin/env bash
# GCP Network Benchmark Setup
# Measures real RTT and coordination overhead across regions
#
# Required environment variables:
#   GCP_PROJECT - Your GCP project ID
#
# Optional:
#   GCP_ZONES - Comma-separated list of zones (default: us-central1-a,us-west1-a,us-east1-b,europe-west1-b)
#   GCP_MACHINE_TYPE - VM machine type (default: e2-medium)
#
# Usage:
#   export GCP_PROJECT=my-project
#   ./scripts/gcp_network_bench.sh

set -e

if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

if [ -z "$GCP_PROJECT" ]; then
    echo "Error: GCP_PROJECT environment variable required"
    echo "Set it directly or create a .env file with: GCP_PROJECT=your-project-id"
    exit 1
fi

PROJECT="$GCP_PROJECT"
IFS=',' read -ra ZONES <<< "${GCP_ZONES:-us-central1-a,us-west1-a,us-east1-b,europe-west1-b}"
MACHINE_TYPE="${GCP_MACHINE_TYPE:-e2-medium}"
IMAGE_FAMILY="debian-12"
IMAGE_PROJECT="debian-cloud"

echo "=== DAG-CRR Network Benchmark ==="
echo "Project: $PROJECT"
echo "Zones: ${ZONES[*]}"

BUILD_LOCALLY=false

if $BUILD_LOCALLY; then
    TARGET="x86_64-unknown-linux-musl"
    if ! rustup target list --installed | grep -q "$TARGET"; then
        rustup target add "$TARGET"
    fi
    cargo build --release --bin network_bench --target "$TARGET"
    BINARY_PATH="target/$TARGET/release/network_bench"
fi

CREATED_VMS=0
create_vm() {
    local zone=$1
    local name="dagcrr-bench-${zone%-*}"

    if gcloud compute instances describe "$name" --project="$PROJECT" --zone="$zone" &>/dev/null; then
        echo "VM $name already exists, skipping"
        return 0
    fi

    echo "Creating VM: $name in $zone..."
    gcloud compute instances create "$name" \
        --project="$PROJECT" \
        --zone="$zone" \
        --machine-type="$MACHINE_TYPE" \
        --image-family="$IMAGE_FAMILY" \
        --image-project="$IMAGE_PROJECT" \
        --tags="dagcrr-bench" \
        --quiet
    CREATED_VMS=$((CREATED_VMS + 1))
}

echo ""
echo "Creating firewall rule..."
gcloud compute firewall-rules create dagcrr-bench-allow \
    --project="$PROJECT" \
    --allow=tcp:9000 \
    --target-tags=dagcrr-bench \
    --quiet 2>/dev/null || true

for zone in "${ZONES[@]}"; do
    create_vm "$zone"
done

if [ $CREATED_VMS -gt 0 ]; then
    echo "Waiting for new VMs to boot..."
    sleep 30
fi

echo ""
echo "=== VM IPs ==="
declare -A VM_IPS
for zone in "${ZONES[@]}"; do
    name="dagcrr-bench-${zone%-*}"
    ip=$(gcloud compute instances describe "$name" \
        --project="$PROJECT" \
        --zone="$zone" \
        --format="get(networkInterfaces[0].accessConfigs[0].natIP)" 2>/dev/null)
    VM_IPS[$zone]=$ip
    echo "$name: $ip"
done

echo ""
if $BUILD_LOCALLY; then
    echo "Uploading binary..."
    for zone in "${ZONES[@]}"; do
        name="dagcrr-bench-${zone%-*}"
        gcloud compute scp --project="$PROJECT" --zone="$zone" \
            "$BINARY_PATH" "$name:~/" --quiet
    done
else
    echo "Building on first VM and distributing..."
    BUILD_ZONE="${ZONES[0]}"
    BUILD_VM="dagcrr-bench-${BUILD_ZONE%-*}"

    gcloud compute scp --project="$PROJECT" --zone="$BUILD_ZONE" \
        --recurse src sync-engine benches Cargo.toml Cargo.lock "$BUILD_VM:~/dag-crr/" --quiet

    gcloud compute ssh "$BUILD_VM" --project="$PROJECT" --zone="$BUILD_ZONE" \
        --command="sudo apt-get update && sudo apt-get install -y build-essential && \
                   (command -v cargo || curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y) && \
                   source ~/.cargo/env && \
                   cd ~/dag-crr && cargo build --release --bin network_bench && \
                   cp target/release/network_bench ~/"

    echo "Downloading binary from build VM..."
    mkdir -p /tmp/dagcrr-build
    gcloud compute scp --project="$PROJECT" --zone="$BUILD_ZONE" \
        "$BUILD_VM:~/network_bench" /tmp/dagcrr-build/network_bench --quiet

    echo "Distributing to other VMs..."
    for i in "${!ZONES[@]}"; do
        [ $i -eq 0 ] && continue
        zone="${ZONES[$i]}"
        name="dagcrr-bench-${zone%-*}"
        gcloud compute scp --project="$PROJECT" --zone="$zone" \
            /tmp/dagcrr-build/network_bench "$name:~/" --quiet &
    done
    wait
fi

echo ""
echo "Starting servers..."
for i in "${!ZONES[@]}"; do
    [ $i -eq 0 ] && continue
    zone="${ZONES[$i]}"
    name="dagcrr-bench-${zone%-*}"
    gcloud compute ssh "$name" --project="$PROJECT" --zone="$zone" \
        --command="chmod +x ~/network_bench && nohup ~/network_bench server --bind 0.0.0.0:9000 > ~/server.log 2>&1 < /dev/null &" \
        -- -f
done

sleep 5

PEERS=""
for i in "${!ZONES[@]}"; do
    [ $i -eq 0 ] && continue
    ip="${VM_IPS[${ZONES[$i]}]}"
    [ -n "$PEERS" ] && PEERS="$PEERS,"
    PEERS="${PEERS}${ip}:9000"
done

echo ""
echo "=== Running Benchmark ==="
CLIENT_ZONE="${ZONES[0]}"
CLIENT_NAME="dagcrr-bench-${CLIENT_ZONE%-*}"
echo "Client: $CLIENT_NAME -> Peers: $PEERS"

gcloud compute ssh "$CLIENT_NAME" --project="$PROJECT" --zone="$CLIENT_ZONE" \
    --command="chmod +x ~/network_bench && ~/network_bench client --peers $PEERS --samples 100 --output ~/results.json"

mkdir -p results
gcloud compute scp --project="$PROJECT" --zone="$CLIENT_ZONE" \
    "$CLIENT_NAME:~/results.json" ./results/network.json --quiet

echo ""
echo "Results: results/network.json"

echo ""
read -p "Delete VMs? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    for zone in "${ZONES[@]}"; do
        name="dagcrr-bench-${zone%-*}"
        gcloud compute instances delete "$name" --project="$PROJECT" --zone="$zone" --quiet &
    done
    wait
    echo "VMs deleted."
fi
