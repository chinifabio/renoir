#!/bin/bash

# Read from environment variables or exit if not set
: ${INTERFACE:=eth0}
: ${BANDWIDTH_RATE:=100mbit}
: ${LATENCY_MS:=10ms}
: ${TARGET_HOSTS:=""}

function get_ip() {
    getent hosts $1 | awk '{ print $1 }'
}

IP_LIST=()
read -ra HOSTS <<< "$TARGET_HOSTS"
for HOST in "${HOSTS[@]}"; do
    IP=$(get_ip $HOST)
    if [ -n "$IP" ]; then
        IP_LIST+=($IP)
    else
        echo "Warning: IP not found for host $HOST"
    fi
done

echo "Applying traffic control rules to interface '$INTERFACE' for targets: ${IP_LIST[*]}"
echo "  Egress/Ingress Bandwidth Limit: $BANDWIDTH_RATE"
echo "  Egress Latency: $LATENCY_MS"

# --- Cleanup existing TC rules ---
echo "Cleaning up existing tc rules on $INTERFACE..."
tc qdisc del dev $INTERFACE root 2>/dev/null || true
tc qdisc del dev $INTERFACE ingress 2>/dev/null || true

# --- Egress Shaping (Upload Limit + Latency) ---
echo "Setting up egress qdisc (upload limit + latency)..."
# 1. Add root HTB qdisc
#    default 10 means traffic not matching filters goes to class 1:10
tc qdisc add dev $INTERFACE root handle 1: htb default 10

# 2. Add parent class matching the total desired rate for limited traffic
#    All limited traffic shares this bandwidth pool.
tc class add dev $INTERFACE parent 1: classid 1:1 htb rate $BANDWIDTH_RATE # ceil $BANDWIDTH_RATE (optional)

# 3. Add a specific class for the target IPs under the parent class
#    This class gets the bandwidth limit.
tc class add dev $INTERFACE parent 1:1 classid 1:2 htb rate $BANDWIDTH_RATE ceil $BANDWIDTH_RATE

# 4. Add netem qdisc for latency ONLY under the target IP class (1:2)
#    handle 20: identifies this netem qdisc
tc qdisc add dev $INTERFACE parent 1:2 handle 20: netem delay ${LATENCY_MS}

# 5. (Optional but Recommended) Add a fair queueing qdisc after netem
#    This helps distribute the delayed bandwidth fairly among flows to target IPs.
#    It attaches to the netem qdisc (parent 20:)
tc qdisc add dev $INTERFACE parent 20: handle 21: fq_codel limit 10240 # Use fq_codel or sfq

# 6. Add default class for non-target traffic (can borrow bandwidth)
#    Give it a low guarantee but allow it to borrow up to the parent rate.
tc class add dev $INTERFACE parent 1:1 classid 1:10 htb rate 1mbit ceil $BANDWIDTH_RATE

# 7. Add filters to direct traffic *to* target IPs to the limited/delayed class (1:2)
echo "Adding egress filters..."
for IP in "${IP_LIST[@]}"; do
  echo "  - Egress filter for destination $IP -> class 1:2"
  tc filter add dev $INTERFACE parent 1: protocol ip prio 1 u32 \
    match ip dst $IP/32 flowid 1:2
done

# --- Ingress Policing (Download Limit - NO IFB) ---
echo "Setting up ingress qdisc (download limit)..."
# 1. Add ingress qdisc
tc qdisc add dev $INTERFACE handle ffff: ingress

# 2. Add filters to police traffic *from* target IPs
#    'police' action drops packets exceeding the rate (less smooth than shaping)
#    'burst' needs tuning - 32k is a starting point. Rate is $BANDWIDTH_RATE.
echo "Adding ingress filters..."
for IP in "${IP_LIST[@]}"; do
  echo "  - Ingress filter for source $IP -> police rate $BANDWIDTH_RATE"
  tc filter add dev $INTERFACE parent ffff: protocol ip prio 1 u32 \
    match ip src $IP/32 \
    action police rate $BANDWIDTH_RATE burst 32k conform-exceed drop
    # alternative burst calc: burst $(( $(echo $BANDWIDTH_RATE | sed 's/[^0-9]*//g') * 1000 / 8 / 10 )) # very rough: rate_in_bytes / 10
done

echo "Traffic control rules applied successfully."

echo "--- Traffic shaping rules applied successfully ---"
echo "Interface: $INTERFACE"
echo "Bandwidth Limit (Up/Down): $BANDWIDTH_RATE"
echo "Outgoing Latency Added: ${LATENCY_MS}ms"
echo "Affected IPs: ${IP_LIST[*]}"