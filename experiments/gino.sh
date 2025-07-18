for bandwidth in 10mbit 100mbit 1gbit 0mbit; do
    for delay in 1ms 10ms 100ms 0ms; do
        echo -e "\e[31mBandwidth: $bandwidth, Delay: $delay\e[0m"
        environment="-e IFACE=eth0"
        # if [ "$DELAY" != "0ms" ]; then
            environment="$environment -e DELAY=$delay"
        # fi
        # if [ "$bandwidth" != "0mbit" ]; then
            environment="$environment -e BANDWIDTH=$bandwidth"
        # fi
        docker exec $environment -e TARGET_HOSTS="worker5 worker6 worker7" -it worker1 /usr/local/bin/shape-network.sh
    done
done
