package apps.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class LargetableTester implements ClientUpdateHandler {
    private String username;
    private String password;
    private List<TesterClient> clients = new ArrayList<>();

    LargetableTester(String username, String password) {
        this.username = username;
        this.password = password;
    }

    void start() throws IOException, InterruptedException {
        /*TesterClient client = new TesterClient("192.168.178.221", username, password, "client1");
        client.setClientUpdateHandler(this);
        clients.add(client);
        client.launch();
        client.startTesting();
        Thread.sleep(1000);
        client.stopTesting();
        client.kill();*/
        TesterServer server = new TesterServer("192.168.178.221", username, password, 0, 0);
        server.launch();
        server.connect();
        server.kill();
    }

    @Override
    public void error(String clientId, int cmdNb, String key, String expected, String actual, String cmdType, String cmdVal) {
        System.out.println("ERROR: client=" + clientId + " key=" + key + " expected=" + expected + " actual=" + actual + " cmdType=" + cmdType + " cmdVal=" + cmdVal);
        for (TesterClient client : clients) {
            client.kill();
        }
    }

    @Override
    public void cmdFinished(String clientId, int commandNumber, String key, String value, String cmdType, String cmdData) {
        System.out.println("Success. client=" + clientId + " cmdNb=" + commandNumber);
    }
}
