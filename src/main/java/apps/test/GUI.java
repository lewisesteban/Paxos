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

// TODO GUI tends to crash
// Happened when clients never died and 2 servers died simultaneously. It seems log grows and snapshots are not created,
// although all servers (and clients) are up. Server froze with a log size of 700. On terminal, got error:
//
// -bash: fork: retry: Resource temporarily unavailable
// That test was not very intense (few kills/restores, maybe only 3 or 4 on servers).
//
// [warning][os,thread] Failed to start thread - pthread_create failed (EAGAIN) for attributes: stacksize: 1024k, guardsize: 0k, detached.
//
// Always happens on the same machine (the one that has 1 extra server).
// ---> Max thread issue? Check for exceptions by running server in foreground.
// --> Also, when deleting acceptor files, make sure to close open ones.

// TODO adapt serial killer to have most of the time only 1 or 2 servers down, sometimes 3, but rarely more
// then i can have a network with 3 fragments, having 3, 3 and 2 servers respectively

// Demonstrate every task done one by one

// Create client that throws in a big dataset

// Server monitor program:
// Give it fragment and node as arguments, then enter key and program returns value
// If a third argument "all" is given, program just returns all keys and their values

// Make sure EC2 servers have already SSH'd each other

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
        setTitle("LargeTable testing GUI");
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

        final Frame frame = this;
        showLoadingWindow("Starting clients and servers." + System.lineSeparator() + "Please wait...");
        Thread serverStartingThread = new Thread(() -> {
            launchServers();
            startClientsSSH();
            closeLoadingWindow();
            for (int i = 0; i < servers.size(); ++i) {
                GUIServerPanel panel = new GUIServerPanel(servers.get(i), i, 250, 140);
                serverPanels.add(panel);
                add(panel);
            }
            for (int i = 0; i < clients.size(); ++i) {
                GUIClientPanel panel = new GUIClientPanel(clients.get(i), i, 0, 140);
                clientPanels.add(panel);
                add(panel);
            }
            setupSKs(clientPanels, serverPanels);
            frame.setVisible(true);
        });
        serverStartingThread.start();
    }

    private void setupSKs(List<GUIClientPanel> clientPanels, List<GUIServerPanel> serverPanels) {
        List<Target> clientTargets = new ArrayList<>();
        for (int i = 0; i < clients.size(); ++i)
            clientTargets.add(new TargetClient(clients.get(i), clientPanels.get(i)));
        clientSK = new GUISerialKillerPanel(clientTargets, 0, 50);
        add(clientSK);

        List<Target> serverTargets = new ArrayList<>();
        for (int i = 0; i < servers.size(); ++i)
            serverTargets.add(new TargetServer(servers.get(i), serverPanels.get(i)));
        serverSK = new GUISerialKillerPanel(serverTargets, 250, 50);
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
