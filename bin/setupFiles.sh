if [ "$#" != 1 ]
then echo "Required argument: IP address of this machine."; exit 0
fi

myIp=$1

if [[ `pwd` = *bin ]]
then cd ..
elif [[ `pwd` != *Paxos ]]
then cd Paxos
fi

read -p "SSH username: " username
read -s -p "SSH password: " password
echo



#
### write local fragment files
#

nbFragments=0
srvId=0
prevFragment="-1"

rm /tmp/paxos_hosts >> /dev/null
rm /tmp/paxos_fragments >> /dev/null

while read line
do
host=$(echo "$line" | cut -f 1 -d " ")
if [ "$host" = "$myIp" ]
then host="127.0.0.1"
fi
fragment=$(echo "$line" | cut -f 2 -d " ")
if [ "$fragment" != "$prevFragment" ]
then srvId=0; echo "$fragment" >> /tmp/paxos_fragments; rm "network_f$fragment" >> /dev/null
fi
prevFragment=$fragment
echo "$host" >> "network_f$fragment"
echo "$host" >> /tmp/paxos_hosts
srvId=$((srvId+1))
done < "network"

sort -u /tmp/paxos_hosts > /tmp/paxos_hosts_unique



#
### write on remote servers
#

host=""

startServer() {
echo "Writing on server " $host;
netFile=$(bin/adaptHosts.sh network $myIp $host)
echo "$netFile"
bin/exp.sh "$password" ssh "$username@$host" "echo \"$netFile\" > Paxos/network" &
while read fragment
do
frgFile=$(bin/adaptHosts.sh network_f$fragment $myIp $host)
bin/exp.sh "$password" ssh "$username@$host" "echo \"$frgFile\" > Paxos/network_f$fragment" &
done < /tmp/paxos_fragments
echo;
}

while read hostLine
do
host=$hostLine
if [ $host != "127.0.0.1" ]
then startServer
fi
done < /tmp/paxos_hosts_unique
