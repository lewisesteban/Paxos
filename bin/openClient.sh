if [ $# != 1 ]
then echo "Argument required: client name"; exit 0
fi

if [[ `pwd` = *bin ]]
then cd ..
fi
java -jar target/paxos_client.jar $1 network
