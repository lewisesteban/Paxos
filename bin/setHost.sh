HOST=`ip addr show | grep "inet " | grep dynamic | cut -d' ' -f 6 | cut -d'/' -f 1`
NB_LINES=`cat /etc/hosts | wc -l`
NAME=`cat /etc/hosts | head -n 2 | tail -n 1 | cut -f 2`
SECOND_LINE="${HOST}\t${NAME}" 

mv /etc/hosts /tmp/oldEtcHosts
cat /tmp/oldEtcHosts | head -n 1 > /etc/hosts
echo $SECOND_LINE >> /etc/hosts
cat /tmp/oldEtcHosts | tail -n $(($NB_LINES - 2)) >> /etc/hosts


