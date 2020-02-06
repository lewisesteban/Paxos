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

// TODO configure serial killer minWait and maxWait

// TODO servers are slow to start

// TODO server 2 dead, last inst 40, client sends cmd, server 1 skips all instances up to 44 because they supposedly have consensus on another value
// happened after doing a command when 2 servers out of 3 are dead, and then trying it again when 2 are up

// TODO after some killing and restarting, restarted all, then did put/appends on server 2, which did not work (gets returned wrong values). However, after killing server 2, gets returned correct values (from server 1)
// put key a
// get key
// kill 2
// append key b
// get key -> b
// was it just a typing mistake?

public class GUI extends Frame {
    private String username, password = null;
    private JWindow loadingWindow = null;
    private List<TesterServer> servers;
    private List<TesterClient> clients;

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
                GUIServerPanel panel = new GUIServerPanel(servers.get(i), i, 250, 100);
                serverPanels.add(panel);
                add(panel);
            }
            for (int i = 0; i < clients.size(); ++i) {
                GUIClientPanel panel = new GUIClientPanel(clients.get(i), i, 0, 100);
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
        add(new GUISerialKillerPanel(clientTargets, 0, 40));

        List<Target> serverTargets = new ArrayList<>();
        for (int i = 0; i < servers.size(); ++i)
            serverTargets.add(new TargetServer(servers.get(i), serverPanels.get(i)));
        add(new GUISerialKillerPanel(serverTargets, 250, 40));
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
                    server.connect();
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
