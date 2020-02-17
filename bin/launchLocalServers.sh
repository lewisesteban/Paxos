if [ "$#" != 1 ]
then echo "Required argument: path to file containing the list of servers."; exit 0
fi

if [[ `pwd` = *bin ]]
then cd ..
fi

srvId=0
prevFragment="-1"

startServer() {
echo "Starting server " $srvId " fragment " $fragment;
bin/startRegistry.sh;
java -jar target/paxos_server.jar $fragment $srvId network_f$fragment &
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
if [ "$host" = "127.0.0.1" ]
then startServer
fi
srvId=$((srvId+1))
done < $1

