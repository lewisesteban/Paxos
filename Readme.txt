--==###########  PREREQUISITES  ###########==--

On each machine/VM, install the following:
- ssh
- java
- maven
- rmiregistry (usually comes with Java)
- expect (the Shell program -- only required for the distributed Shell scripts)

For the testing GUI and distributed shell scripts to work, each machine needs to have a common user, and allow this user
to connect with SSH using password authentication. To allow SSH password connection, find the file /etc/ssh/sshd_config
and add/edit the following line (without quotes):
"PasswordAuthentication yes"
You can then restart SSH with the following command: sudo service ssh restart

For the distributed scripts to work, you also need to add each host to the list of known hosts for the SSH connection to
work. You can do that by SSH'ing to every host once. The host will be added the first time you try to establish an SSH
connection.

You might also want to edit the files "/etc/security/limits.conf" and "/proc/sys/kernel/threads-max" to allow for more
threads.

/!\ MAKE SURE YOU HAVE AT LEAST 1 GB OF FREE MEMORY PER SERVER PROCESS
If you intend to store large amounts of data, allow for at least at an extra two times the amount of data stored.

Don't forget to handle any firewall or security system (like the "security group" in the case of AWS EC2) to allow for
RMI communications.



--==###############   RUN   ###############==--

Step A. Compile with Maven without running tests (you can use the script "install.sh" to do so).

Step B. Create the required files.
Servers require a fragment file (usually called "network_f[fragmentNb]"), and clients a global file (usually called
"network"). Here is an example for one fragment (numbered 0) with 1 node on host 1.2.3.4 and another fragment
(numbered 1) with two nodes on hosts 1.2.3.4 and 5.6.7.8. We can create a file called "network" for the client, with the
following content (3 lines, one per server):
1.2.3.4 0
1.2.3.4 1
5.6.7.8 1
For the server of the first fragment, we can create the following file, called for "network_f0" (one line):
1.2.3.4
For the servers of the second fragment, we can create the file "network_f1" (two lines):
1.2.3.4
5.6.7.8

Step C. Run "rmiregistry" in the directory "target/classes"

Step D. Run the server and client jar files. Arguments:
- The servers need to know their fragment number and ID within the fragment. This ID is their 0-based index in the file.
In the above example, the ID of the server running on host 5.6.7.8 is 1, and the ID of the two others is 0.
- The clients need a unique client ID, which can be any string.
- Clients and servers can take as last argument the path to the file described in step B.



--==############  TESTING GUI  ############==--

To run the testing GUI, you need to be able to SSH into all machines with password authentication (see Prerequisites).

You also need a file called "network", which corresponds to the requirements of a client file (see Run, Step B.).

Additionally, you need a file called "tester_clients" which contains the list of the clients that will be created by the
tester program. Every line contains first the client's host and then its unique ID, separated by a space. For instance:
15.188.60.247 client1
35.180.230.8 client2
35.180.230.8 client3



--==###########  SHELL SCRIPTS  ###########==--

A number of shell scripts have been written to make life easier. They are located in "bin".

clean.sh                Removes all files created by Paxos clients and servers at the current directory.
install.sh              Uses maven to compile the client and server without running tests.
killLocalServers.sh     Kills all running servers on the local host.
launchAllServers.sh     Launches all servers on all hosts.
launchLocalServers.sh   Launches servers on the local host.
openClient.sh           Runs the client. The client will read the file called "network".
setupFiles.sh           Reads a "network" file (see Run, Step B) and creates all other files on every host accordingly.
startRegistry.sh        Starts the RMI registry at the correct location if it isn't running yet.

Additionally, you can check if any Paxos processes are running by using the following simple Shell command:
ps axo pid,cmd | grep java