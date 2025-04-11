#!/bin/bash

: ${TARGET_HOSTS:=""}

function get_ip() {
    getent hosts $1 | awk '{ print $1 }'
}

TARGET_IPS=()
read -ra HOSTS <<< "$TARGET_HOSTS"
for HOST in "${HOSTS[@]}"; do
    IP=$(get_ip $HOST)
    if [ -n "$IP" ]; then
        TARGET_IPS+=($IP)
        echo "Found IP $IP for host $HOST"
    else
        echo "Warning: IP not found for host $HOST"
    fi
done

# Read interface from environment variable or exit if not set
IFACE="${IFACE:-}"
if [ -z "$IFACE" ]; then
  echo "Network interface (IFACE) not set"
  exit 1
fi

# Optional delay and bandwidth, can be empty (no shaping if not set)
DELAY="${DELAY:-}"
BANDWIDTH="${BANDWIDTH:-}"

# Clear existing qdisc
tc qdisc del dev "$IFACE" root 2>/dev/null

# No shaping if both DELAY and BANDWIDTH are empty
if [ -z "$DELAY" ] && [ -z "$BANDWIDTH" ]; then
  echo "No bandwidth or delay specified. No shaping applied."
  exit 0
fi

# Create root qdisc and class if bandwidth is specified
if [ -n "$BANDWIDTH" ]; then
  echo "Setting up root HTB qdisc with bandwidth $BANDWIDTH"
  tc qdisc add dev "$IFACE" root handle 1: htb default 30
  tc class add dev "$IFACE" parent 1: classid 1:1 htb rate $BANDWIDTH
  PARENT="1:1"
else
  # Only delay: use netem directly at root
  echo "Setting up root netem qdisc with delay $DELAY"
  tc qdisc add dev "$IFACE" root handle 1: netem delay $DELAY
  PARENT="1:"
fi

# Add netem qdisc for delay if bandwidth is set
if [ -n "$DELAY" ] && [ -n "$BANDWIDTH" ]; then
  echo "Adding netem with delay $DELAY"
  tc qdisc add dev "$IFACE" parent $PARENT handle 10: netem delay $DELAY
  PARENT="10:"
fi

# Apply filters if TARGET_IPS is set
if [ -n "$TARGET_IPS" ]; then
  for ip in "${TARGET_IPS[@]}"; do
    echo "Applying shaping to target $ip"
    tc filter add dev "$IFACE" protocol ip parent 1:0 prio 1 u32 match ip dst $ip flowid 1:1
    tc filter add dev "$IFACE" protocol ip parent 1:0 prio 1 u32 match ip src $ip flowid 1:1
  done
else
  echo "No TARGET_IPS specified. Applying shaping to all traffic."
fi