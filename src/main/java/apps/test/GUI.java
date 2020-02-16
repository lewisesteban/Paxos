package apps.test;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

// TODO change network_fragment file name according to fragment

// TODO GUI display

// TODO test redirection (client is talking to 1 but 2 is back, is client redirected?)

// TODO error
// client got stuck on AGAIN after having killed 2 servers simultaneously

// TODO error
// kill and restore while doing commands, then all servers are up everything is well
// then kill 2. here's what happens:
// #client1  in:append client1_key2 2
// #client1 out:ERR network. com.lewisesteban.paxos.client.ClientCommandSender$CommandFailedException: java.io.IOException: java.rmi.ConnectIOException: error during JRMP connection establishment; nested exception is:
// #client1  in:append client1_key2 2
// #client1 out:	java.net.SocketException: Connection reset
// #client1  in:append client1_key2 2
// #client1 out:OK
// #client1  in:get client1_key2
// #client1 out:OK
// #client1 key client1_key2 local val is 6792401482345802
// #client1  in:put client1_key0 3
// ERROR key=client1_key2
//
// PS: in all the log, there is no other JRMP error or Connection reset exception

// TODO error
// after many failures (error mentioned below), client seems to treat every request as a GET
// ==> MAY BE BECAUSE THE CLIENT SENT "GET" REQUESTS AFTER EVERY FAILURE, INSTEAD OF TRY AGAIN
// #client1  in:append client1_key2 3
// #client1 out:OK 35745689024356867235658952364579172
// #client1  in:get client1_key2
// #client1 out:OK 35745689024356867235658952364579172
// #client1 key client1_key2 local val is 357456890243568672356589523645791723
// ERROR key=client1_key2

// TODO (minor) error: two servers are up, but the client keeps failing
// #client1 out:ERR network. com.lewisesteban.paxos.client.ClientCommandSender$CommandFailedException: java.io.IOException: java.rmi.ConnectException: Connection refused to host: 192.168.178.221; nested exception is:
// #client1  in:get client1_key2
// #client1 out:	java.net.ConnectException: Connection refused (Connection refused)
// #client1  in:get client1_key2


public class GUI extends Frame {
    private String username, password = null;
    private JWindow loadingWindow = null;
    private List<TesterServer> servers;
    private List<TesterClient> clients;
    private GUISerialKillerPanel clientSK, serverSK;

    public static void main(String... args) throws IOException {
        new GUI();
    }

    private GUI() throws IOException {
        promptCredentials();
        setVisible(true);
        setLayout(null);
        setSize(600, 500);
        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                close();
            }
        });
        setupClientsAndServers();
    }

    private void setupClientsAndServers() throws IOException {
        Factory factory = new Factory("tester_clients", "network");
        servers = factory.createServers();
        clients = factory.createClients();

        List<GUIServerPanel> serverPanels = new ArrayList<>();
        List<GUIClientPanel> clientPanels = new ArrayList<>();

        showLoadingWindow("Starting clients and servers." + System.lineSeparator() + "Please wait...");
        Thread serverStartingThread = new Thread(() -> {
            launchServers();
            startClientsSSH();
            closeLoadingWindow();
            for (int i = 0; i < servers.size(); ++i) {
                GUIServerPanel panel = new GUIServerPanel(servers.get(i), i, 250, 130);
                serverPanels.add(panel);
                add(panel);
            }
            for (int i = 0; i < clients.size(); ++i) {
                GUIClientPanel panel = new GUIClientPanel(clients.get(i), i, 0, 130);
                clientPanels.add(panel);
                add(panel);
            }
            setupSKs(clientPanels, serverPanels);
        });
        serverStartingThread.start();
    }

    private void setupSKs(List<GUIClientPanel> clientPanels, List<GUIServerPanel> serverPanels) {
        List<Target> clientTargets = new ArrayList<>();
        for (int i = 0; i < clients.size(); ++i)
            clientTargets.add(new TargetClient(clients.get(i), clientPanels.get(i)));
        clientSK = new GUISerialKillerPanel(clientTargets, 0, 40);
        add(clientSK);

        List<Target> serverTargets = new ArrayList<>();
        for (int i = 0; i < servers.size(); ++i)
            serverTargets.add(new TargetServer(servers.get(i), serverPanels.get(i)));
        serverSK = new GUISerialKillerPanel(serverTargets, 250, 40);
        add(serverSK);
    }

    private void showLoadingWindow(String msg) {
        Panel contents = new Panel();
        Label text = new Label(msg);
        contents.add(text);
        Button cancelBtn = new Button("Cancel");
        cancelBtn.addActionListener(e -> close());
        cancelBtn.setSize(80, 25);
        contents.add(cancelBtn);
        loadingWindow = new JWindow();
        loadingWindow.getContentPane().add(contents);
        loadingWindow.setBounds(100, 100, 300, 100);
        loadingWindow.setVisible(true);
    }

    private void closeLoadingWindow() {
        loadingWindow.setVisible(false);
        loadingWindow.dispose();
    }

    private void launchServers() {
        CyclicBarrier barrier = new CyclicBarrier(servers.size());
        List<Thread> allThreads = new ArrayList<>();
        for (TesterServer server : servers) {
            Thread serverThread = new Thread(() -> {
                try {
                    server.startSSH(username, password);
                    server.launch();
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException | IOException e) {
                    e.printStackTrace();
                    close();
                }
            });
            allThreads.add(serverThread);
            serverThread.start();
        }
        for (Thread thread : allThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void startClientsSSH() {
        List<Thread> allThreads = new ArrayList<>();
        for (TesterClient client : clients) {
            Thread thread = new Thread(() -> {
                try {
                    client.startSSH(username, password);
                } catch (IOException e) {
                    e.printStackTrace();
                    close();
                }
            });
            thread.start();
            allThreads.add(thread);
        }
        for (Thread thread : allThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void promptCredentials() {
        username = JOptionPane.showInputDialog(this, "Enter SSH username", "SSH User", JOptionPane.PLAIN_MESSAGE);
        if (username == null)
            System.exit(0);

        JPasswordField jpf = new JPasswordField(24);
        JLabel jl = new JLabel("Enter Your SSH Password: ");
        Box box = Box.createHorizontalBox();
        box.add(jl);
        box.add(jpf);
        int x = JOptionPane.showConfirmDialog(this, box, "Password", JOptionPane.OK_CANCEL_OPTION);
        if (x == JOptionPane.OK_OPTION) {
            password = new String(jpf.getPassword());
        } else {
            System.exit(0);
        }
    }

    private synchronized void close() {
        clientSK.turnOff();
        serverSK.turnOff();
        if (clients != null) {
            for (TesterClient client : clients)
                client.kill();
        }
        if (servers != null) {
            for (TesterServer server : servers)
                server.kill();
        }
        dispose();
        System.exit(0);
    }
}
