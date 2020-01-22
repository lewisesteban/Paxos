package apps.test;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

class TesterServer {
    private int nodeId;
    private int fragmentId;
    private final SSHClient sshClient = new SSHClient();
    private Session.Command largetableProcess = null;
    private BufferedReader reader = null;
    private String pid = null;

    TesterServer(String host, String username, String password, int nodeId, int fragmentId) throws IOException {
        this.nodeId = nodeId;
        this.fragmentId = fragmentId;
        sshClient.addHostKeyVerifier(new PromiscuousVerifier());
        sshClient.connect(host);
        sshClient.authPassword(username, password);
    }

    synchronized boolean launch() {
        try {
            Session session = sshClient.startSession();
            String cmdLine = "java -jar Paxos/target/paxos_server.jar " + fragmentId + " " + nodeId + " Paxos/network_fragment";
            largetableProcess = session.exec(cmdLine);
            reader = new BufferedReader(new InputStreamReader(largetableProcess.getInputStream()));
            reader.readLine(); // this line should be a message stating that RMI server has started
            getPid(cmdLine);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void getPid(String startProcessCmd) throws IOException {
        Session session = sshClient.startSession();
        String getPidCmd = "ps axo pid,cmd | grep \"" + startProcessCmd + "\" | head -n1 | cut -d \" \" -f1";
        pid = IOUtils.readFully(session.exec(getPidCmd).getInputStream()).toString();
    }

    synchronized boolean connect() {
        try {
            largetableProcess.getOutputStream().write("c\n".getBytes());
            largetableProcess.getOutputStream().flush();
            reader.readLine(); // the next line should be "Server ready"
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    synchronized boolean isUp() {
        return largetableProcess != null;
    }

    synchronized boolean kill() {
        try {
            Session session = sshClient.startSession();
            IOUtils.readFully(session.exec("kill -9 " + pid).getInputStream());
            largetableProcess = null;
            return true;
        } catch (ConnectionException e) {
            e.printStackTrace();
            largetableProcess = null;
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
