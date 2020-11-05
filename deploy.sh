name=$1
ip=$2
port=$3
view=$4 
addr="${ip}:13800"

docker stop "${name}"
docker rm "${name}"

docker build -t kv-store:5.0 .

clear

docker run --network=kv_subnet                           \
		   --name="${name}"                              \
           --ip="${ip}"          -p "${port}":13800/udp  \
           -e ADDRESS="${addr}"  -p "${port}":13800      \
           -e REPL_FACTOR=2							     \
           -e VIEW="${view}"                             \
           kv-store:5.0  
