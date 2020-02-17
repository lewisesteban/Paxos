if [ "$#" != 1 ]
then echo "Required argument: path to file containing the list of servers."; exit 0
fi

read -p "SSH username: " username
read -s -p "SSH password: " password
echo

srvId=0
prevFragment="-1"

startServer() {
echo "Starting server " $srvId " fragment " $fragment " on host " $host;
cmd="Paxos/bin/startRegistry.sh; java -jar Paxos/target/paxos_server.jar $fragment $srvId Paxos/network_f$fragment &"
bin/exp.sh "$password" ssh "$username@$host" $cmd &
echo;
}

while read line
do
host=$(echo "$line" | cut -f 1 -d " ")
fragment=$(echo "$line" | cut -f 2 -d " ")
if [ "$fragment" != "$prevFragment" ]
then srvId=0
fi
prevFragment=$fragment
startServer
srvId=$((srvId+1))
done < $1

