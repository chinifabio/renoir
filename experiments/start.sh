#!/bin/bash

# Set default values for SSH_USERNAME and SSH_PASSWORD if not provided
: ${SSH_USERNAME:=ubuntu}
: ${SSH_PASSWORD:?"Error: SSH_PASSWORD environment variable is not set."}
: ${SSHD_CONFIG_ADDITIONAL:=""}

# Create the user with the provided username and set the password
if id "$SSH_USERNAME" &>/dev/null; then
    echo "User $SSH_USERNAME already exists"
else
    useradd -ms /bin/bash "$SSH_USERNAME"
    echo "$SSH_USERNAME:$SSH_PASSWORD" | chpasswd
    echo "User $SSH_USERNAME created with the provided password"
fi

# Set the authorized keys from the AUTHORIZED_KEYS environment variable (if provided)
if [ -n "$AUTHORIZED_KEYS" ]; then
    mkdir -p /home/$SSH_USERNAME/.ssh
    echo "$AUTHORIZED_KEYS" > /home/$SSH_USERNAME/.ssh/authorized_keys
    chown -R $SSH_USERNAME:$SSH_USERNAME /home/$SSH_USERNAME/.ssh
    chmod 700 /home/$SSH_USERNAME/.ssh
    chmod 600 /home/$SSH_USERNAME/.ssh/authorized_keys
    echo "Authorized keys set for user $SSH_USERNAME"
    # Disable password authentication if authorized keys are provided
    sed -i 's/PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config
fi

# Apply additional SSHD configuration if provided
if [ -n "$SSHD_CONFIG_ADDITIONAL" ]; then
    echo "$SSHD_CONFIG_ADDITIONAL" >> /etc/ssh/sshd_config
    echo "Additional SSHD configuration applied"
fi

# Apply additional SSHD configuration from a file if provided
if [ -n "$SSHD_CONFIG_FILE" ] && [ -f "$SSHD_CONFIG_FILE" ]; then
    cat "$SSHD_CONFIG_FILE" >> /etc/ssh/sshd_config
    echo "Additional SSHD configuration from file applied"
fi

function get_ip() {
    getent hosts $1 | awk '{ print $1 }'
}

# Allowed high-speed IPs
: ${ALLOWED_HOSTS:=""}
ALLOWED_IPS=()

IFS=',' read -ra HOSTS <<< "$ALLOWED_HOSTS"
for HOST in "${HOSTS[@]}"; do
    IP=$(get_ip $HOST)
    if [ -n "$IP" ]; then
        ALLOWED_IPS+=($IP)
    else
        echo "Warning: IP not found for host $HOST"
    fi
done

# Clear existing rules
tc qdisc del dev $DEV root 2>/dev/null

# Define delays
: ${SLOW_DELAY:="10ms"}  # Default delay for all traffic
FAST_DELAY=1ms   # Reduced delay for specific hosts

# Define device
DEV=eth0

# Cleanup existing rules
tc qdisc del dev $DEV root 2>/dev/null

# Add root qdisc
tc qdisc add dev $DEV root handle 1: prio

# Add a netem qdisc for slow traffic
tc qdisc add dev $DEV parent 1:3 handle 30: netem delay $SLOW_DELAY

# Add a netem qdisc for fast traffic
tc qdisc add dev $DEV parent 1:2 handle 10: netem delay $FAST_DELAY

# Apply fast delay to specific hosts
for IP in "${ALLOWED_IPS[@]}"; do
    tc filter add dev $DEV protocol ip parent 1:0 prio 1 u32 match ip dst $IP flowid 1:2
done

# Redirect all traffic to slow delay by default
tc filter add dev $DEV protocol ip parent 1:0 prio 2 u32 match ip dst 0.0.0.0/0 flowid 1:3

# Start the SSH server
echo "Starting SSH server..."
exec /usr/sbin/sshd -D