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

pub_key=$(cat ./id_rsa.pub)

docker compose --env-file .env -f netsim-compose.yaml down
sleeppp 5

for bandwidth in 10mbit 100mbit 1gbit
do
  for delay in 1ms 20ms 100ms
  do
      cp .env.sample .env
      sed -i "s/key/AUTHORIZED_KEYS=$pub_key/g" .env
      sed -i "s/latency/LATENCY_MS=$delay/g" .env
      sed -i "s/bandwidth/BANDWIDTH_RATE=$bandwidth/g" .env
      docker compose --env-file .env -f netsim-compose.yaml up -d
      sleeppp 10
      # docker exec -it launcher sh -c "ssh-keyscan worker1 worker2 worker3 worker4 worker5 worker6 worker7 >> ~/.ssh/known_hosts"
      docker exec -it launcher sh -c 'echo -e "Host *\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile /dev/null" >> ~/.ssh/config'
      sleeppp 20
      docker exec -it launcher hyperfine -L size 100000,1000000,10000000 -L type flowunits,renoir --warmup 3 --export-json results/$delay-$bandwidth.json --show-output "./flowunits -r config_{type}.toml -- {size}"
      docker compose --env-file .env -f netsim-compose.yaml down
      sleeppp 5
  done
done