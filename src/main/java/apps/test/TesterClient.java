package apps.test;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

class TesterClient {
    private static final int NB_ENTRIES = 3;

    private String host;
    private String clientId;
    private final SSHClient sshClient = new SSHClient();
    private Session session = null;
    private Session.Command largetableProcess = null;
    private String pid = null;
    private Thread testingThread = null;
    private boolean testing = false;
    private BufferedReader reader = null;
    private boolean isAuthenticated = false;

    private Random random = new Random();
    private Map<String, String> testingValues = new TreeMap<>();
    private ClientUpdateHandler clientUpdateHandler;
    private int commandCounter = 0;

    private int cmdVal = 0;
    private String lastAppliedCmd = null;

    TesterClient(String host, String clientId) {
        this.clientId = clientId;
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
            String cmdLine = "java -jar Paxos/target/paxos_client.jar " + clientId + " Paxos/network";
            largetableProcess = session.exec(cmdLine);
            reader = new BufferedReader(new InputStreamReader(largetableProcess.getInputStream()));
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

    private void fetchRemoteValues() throws IOException {
        System.out.println("#" + this.clientId + "----- fetching remote values");
        for (int i = 0; i < NB_ENTRIES; i++) {
            String key = clientId + "_key" + i;
            String resLine = doCommandUntilSuccess("get " + key + "\n");
            if (resLine.length() > 3) {
                String val = resLine.substring(3);
                testingValues.put(key, val);
            }
        }
    }

    synchronized void startTesting() {
        testing = true;
        testingThread = new Thread(() -> {
            try {
                if (testingValues.isEmpty())
                    fetchRemoteValues();
                System.out.println("#" + this.clientId + "----- restoring");
                restore();
                System.out.println("#" + this.clientId + "----- starting");
                while (largetableProcess != null && largetableProcess.isOpen() && testing) {

                    // choose a write command (put or append)
                    String key = clientId + "_key" + random.nextInt(NB_ENTRIES);
                    String cmdType = random.nextInt(10) == 0 ? "put" : "append";
                    String cmdVal = nextVal();
                    String cmd = cmdType + " " + key + " " + cmdVal + "\n";

                    // execute it
                    doCommandUntilSuccess(cmd);
                    applyToLocalDb(cmd);

                    // do a get command to check
                    if (random.nextInt(3) == 0) {
                        String res = doCommandUntilSuccess("get " + key + "\n");
                        String resVal = res.length() <= 3 ? null : res.substring(3);
                        System.out.println("#" + this.clientId + " key " + key + " local val is " + testingValues.get(key));
                        if ((resVal == null && testingValues.get(key) == null) ||
                                (resVal != null && resVal.equals(testingValues.get(key)))) {
                            cmdFinished(key, resVal, cmdType, cmdVal);
                            cmdFinished(key, resVal, "get", null);
                        } else {
                            reportError(key, testingValues.get(key), resVal, cmdType, cmdVal);
                        }
                    } else {
                        cmdFinished(key, testingValues.get(key), cmdType, cmdVal);
                    }
                }
            } catch (IOException e) {
                // typically happens when client process is killed by tester program
                testing = false;
            }
        });
        testingThread.start();
    }

    private String nextVal() {
        cmdVal = (cmdVal + 1) % 10;
        return Integer.toString(cmdVal);
    }

    private void restore() throws IOException {
        String lastFinishedCmd = doCommandUntilSuccess("last command");
        if (lastFinishedCmd.length() <= 3)
            return;
        lastFinishedCmd = lastFinishedCmd.toLowerCase().substring(3).replace("\"", "") + "\n";
        if (!lastFinishedCmd.equals(lastAppliedCmd)) {
            applyToLocalDb(lastFinishedCmd);
        }
    }

    private void applyToLocalDb(String command) {
        String[] words = command.toLowerCase().substring(0, command.length() - 1).split(" ");
        String type = words[0];
        if (type.equals("get"))
            return;
        String key = words[1];
        String val = words[2];
        if (type.equals("put") || !testingValues.containsKey(key))
            testingValues.put(key, val);
        else
            testingValues.put(key, testingValues.get(key) + val);
        lastAppliedCmd = command;
    }

    private synchronized String doCommandUntilSuccess(String command) throws IOException {
        if (largetableProcess == null)
            throw new IOException("connection closed");
        if (!command.endsWith("\n"))
            command += "\n";
        String resLine;
        do {
            largetableProcess.getOutputStream().write(command.getBytes());
            System.out.print("#" + this.clientId + "  in:" + command);
            largetableProcess.getOutputStream().flush();
            resLine = reader.readLine();
            System.out.println("#" + this.clientId + " out:" + resLine);
        } while (testing && resLine != null && !resLine.startsWith("OK"));
        if (resLine == null)
            throw new IOException("connection closed");
        return resLine;
    }

    String getClientId() {
        return clientId;
    }

    synchronized boolean isUp() {
        return largetableProcess != null;
    }

    synchronized boolean isTesting() {
        return testing;
    }

    int getNbFinishedCommands() {
        return commandCounter;
    }

    synchronized void stopTesting() {
        testing = false;
        if (testingThread != null) {
            try {
                testingThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            testingThread = null;
        }
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
                stopTesting();
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

    private void reportError(String key, String expected, String actual, String cmdType, String cmdVal) {
        System.err.println("ERROR key=" + key);
        clientUpdateHandler.error(clientId, commandCounter, key, expected, actual, cmdType, cmdVal);
    }

    private void cmdFinished(String key, String val, String cmdType, String cmdVal) {
        clientUpdateHandler.cmdFinished(clientId, commandCounter, key, val, cmdType, cmdVal);
        commandCounter++;
    }

    void setClientUpdateHandler(ClientUpdateHandler clientUpdateHandler) {
        this.clientUpdateHandler = clientUpdateHandler;
    }
}
