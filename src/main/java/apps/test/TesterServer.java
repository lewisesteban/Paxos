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
    private String host;
    private int nodeId;
    private int fragmentId;
    private final SSHClient sshClient = new SSHClient();
    private Session session = null;
    private Session.Command largetableProcess = null;
    private String pid = null;
    private boolean isAuthenticated = false;

    TesterServer(String host, int nodeId, int fragmentId) {
        this.nodeId = nodeId;
        this.fragmentId = fragmentId;
        this.host = host;
    }

    synchronized void startSSH(String username, String password) throws IOException {
        sshClient.addHostKeyVerifier(new PromiscuousVerifier());
        sshClient.connect(host);
        sshClient.authPassword(username, password);
        isAuthenticated = true;
    }

    synchronized boolean launch() {
        try {
            session = sshClient.startSession();
            String cmdLine = "java -jar Paxos/target/paxos_server.jar " + fragmentId + " " + nodeId + " Paxos/network_f" + fragmentId;
            largetableProcess = session.exec(cmdLine);
            BufferedReader reader = new BufferedReader(new InputStreamReader(largetableProcess.getInputStream()));
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
        String getPidCmd = "ps axo pid,cmd | grep \"" + startProcessCmd + "\" | grep -P '\\d+' -o | head -n1";
        pid = IOUtils.readFully(session.exec(getPidCmd).getInputStream()).toString();
    }

    synchronized boolean isUp() {
        return largetableProcess != null;
    }

    boolean kill() {
        if (!isAuthenticated)
            return true;
        try {
            Session killSession = sshClient.startSession();
            IOUtils.readFully(killSession.exec("kill -9 " + pid).getInputStream());
            killSession.close();
            synchronized (this) {
                if (largetableProcess != null) {
                    largetableProcess.close();
                    largetableProcess = null;
                }
                if (session != null) {
                    session.close();
                    session = null;
                }
            }
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

    public int getNodeId() {
        return nodeId;
    }

    public int getFragmentId() {
        return fragmentId;
    }
}
