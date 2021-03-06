package apps.test;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

class TesterServer {
    private String host;
    private int nodeId;
    private int fragmentId;
    private String username, password;

    private SSHClient sshClient = new SSHClient();
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
        this.username = username;
        this.password = password;
        sshClient.addHostKeyVerifier(new PromiscuousVerifier());
        sshClient.connect(host);
        sshClient.authPassword(username, password);
        isAuthenticated = true;
    }

    private synchronized void reconnect() {
        isAuthenticated = false;
        try {
            sshClient.close();
        } catch (IOException ignored) {
        }
        sshClient = new SSHClient();
        sshClient.addHostKeyVerifier(new PromiscuousVerifier());
        try {
            sshClient.connect(host);
            sshClient.authPassword(username, password);
            isAuthenticated = true;
        } catch (IOException e) {
            System.out.println("Reconnect failed: " + e);
        }
    }

    synchronized boolean launch() {
        if (largetableProcess != null)
            return false;
        try {
            session = sshClient.startSession();
            String cmdLine = "java -jar Paxos/target/paxos_server.jar " + fragmentId + " " + nodeId + " Paxos/network_f" + fragmentId;
            largetableProcess = session.exec(cmdLine);
            new BufferedReader(new InputStreamReader(largetableProcess.getInputStream()));
            getPid(cmdLine);
            return true;
        } catch (ConnectionException | IllegalStateException e) {
            reconnect();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void getPid(String startProcessCmd) throws IOException {
        Session session = sshClient.startSession();
        String getPidCmd = "ps axo pid,cmd | grep \"" + startProcessCmd + "\" | grep -v \"grep\" | grep -P '\\d+' -o | head -n1";
        pid = IOUtils.readFully(session.exec(getPidCmd).getInputStream()).toString();
        session.close();
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
            if (largetableProcess != null) {
                try {
                    largetableProcess.close();
                } catch (TransportException | ConnectionException e) {
                    System.out.println("server process close error: " + e);
                }
                largetableProcess = null;
            }
            if (session != null) {
                try {
                    session.close();
                } catch (TransportException | ConnectionException e) {
                    System.out.println("server session close error: " + e);
                }
                session = null;
            }
            return true;
        } catch (ConnectionException | IllegalStateException e) {
            reconnect();
            return false;
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
