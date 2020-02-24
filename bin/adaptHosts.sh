cat $1 > /tmp/adaptHosts
sed -i -e "s/127.0.0.1/$2/g" /tmp/adaptHosts
sed -i -e "s/$3/127.0.0.1/g" /tmp/adaptHosts
cat /tmp/adaptHosts
