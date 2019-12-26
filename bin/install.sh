if [[ `pwd` = *bin ]]
then cd ..
fi
mvn -Dmaven.test.skip=true package
