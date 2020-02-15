if [[ `ps axo cmd | grep rmiregistry | head -n 1` = "rmiregistry" ]]
then exit 0
fi

if [[ `pwd` = *bin ]]
then cd ..
elif [[ `pwd` != *Paxos ]]
then cd Paxos
fi
cd target/classes
rmiregistry&
