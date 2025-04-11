sleeppp() {
  local duration=$1
  local spin_chars='|/-\'
  local i=0

  echo -n "Sleeping for $duration seconds "

  while [ "$duration" -gt 0 ]; do
    # Print spinner and countdown
    local spin_char=${spin_chars:i++%${#spin_chars}:1}
    echo -ne "\r$spin_char  ${duration}s left"
    sleep 1
    ((duration--))
  done

  echo -e "\r✓  Done sleeping!       "
}

cargo build --release --example flowunits
cp ../target/release/examples/flowunits ./flowunits
echo "build flowunits done"

if [ ! -d "results" ]; then
    mkdir results
fi

docker compose --env-file .env -f netsim-compose.yaml down
sleeppp 5

# NOTE: compose up and down can be done only once because the shape-network script
for bandwidth in 10mbit 100mbit 1gbit 0mbit
do
  for delay in 1ms 10ms 100ms 0ms
  do
      docker compose --env-file .env -f netsim-compose.yaml up -d
      sleeppp 10
      docker exec -it launcher mkdir -p /root/.ssh
      sleeppp 5
      docker exec -it launcher sh -c 'echo -e "Host *\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile /dev/null" >> /root/.ssh/config'
      sleeppp 20

      environment="-e IFACE=eth0"
      if [ "$DELAY" != "0ms" ]; then
          environment="$environment -e DELAY=$delay"
      fi
      if [ "$bandwidth" != "0mbit" ]; then
          environment="$environment -e BANDWIDTH=$bandwidth"
      fi

      # layer edge
      docker exec $environment -e TARGET_HOSTS="worker5 worker6 worker7" -it worker1 /usr/local/bin/shape-network.sh
      sleeppp 5
      docker exec $environment -e TARGET_HOSTS="worker5 worker6 worker7" -it worker2 /usr/local/bin/shape-network.sh
      sleeppp 5
      docker exec $environment -e TARGET_HOSTS="worker5 worker6 worker7" -it worker3 /usr/local/bin/shape-network.sh
      sleeppp 5
      docker exec $environment -e TARGET_HOSTS="worker5 worker6 worker7" -it worker4 /usr/local/bin/shape-network.sh
      sleeppp 5

      # layer site
      docker exec $environment -e TARGET_HOSTS="worker1 worker2 worker3 worker4 worker7" -it worker5 /usr/local/bin/shape-network.sh
      sleeppp 5
      docker exec $environment -e TARGET_HOSTS="worker1 worker2 worker3 worker4 worker7" -it worker6 /usr/local/bin/shape-network.sh
      sleeppp 5

      # layer cloud
      docker exec $environment -e TARGET_HOSTS="worker1 worker2 worker3 worker4 worker5 worker6" -it worker7 /usr/local/bin/shape-network.sh
      sleeppp 5


      docker exec -it launcher hyperfine -L size 100000,1000000,10000000 -L type flowunits,renoir --warmup 3 --export-json results/$delay-$bandwidth.json --show-output "./flowunits -r config_{type}.toml -- {size}"
      docker compose --env-file .env -f netsim-compose.yaml down
      sleeppp 5
  done
done