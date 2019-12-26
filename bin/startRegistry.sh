if [[ `pwd` = *bin ]]
then cd ..
fi
cd target/classes
rmiregistry&
