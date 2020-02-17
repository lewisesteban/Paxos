lines=$(ps axo pid,cmd | grep "paxos_server.jar" | grep "java -jar" | grep -v "grep" | grep -Eo '^ [^ ]+')
for line in $lines
do
echo "Killing $line"
kill $line
done

lines=$(ps axo pid,cmd | grep "paxos_server.jar" | grep "java -jar" | grep -v "grep" | grep -Eo '^[^ ]+')
for line in $lines
do
echo "Killing $line"
kill $line
done
