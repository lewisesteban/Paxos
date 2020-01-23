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

public class GUI extends Frame {
    private List<TesterServer> servers;
    private List<TesterClient> clients;
    private String username = null;
    private String password = null;
    private JWindow loadingWindow = null;

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

        Factory factory = new Factory("tester_clients", "network");

        servers = factory.createServers();
        clients = factory.createClients();

        showLoadingWindow("Starting clients and servers." + System.lineSeparator() + "Please wait...");
        Thread serverStartingThread = new Thread(() -> {
            launchServers();
            startClientsSSH();
            closeLoadingWindow();
            for (int i = 0; i < servers.size(); ++i)
                add(new GUIServerPanel(servers.get(i), i));
            for (int i = 0; i < clients.size(); ++i)
                add(new GUIClientPanel(clients.get(i), i));
        });
        serverStartingThread.start();
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
