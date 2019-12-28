package apps.test;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.io.Console;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class SSHTest {

    public static void main(String... args) throws IOException {

        String username = getUsername();
        if (username == null)
            return;
        String password = getPassword();
        System.out.println(username + " " + password);

        final SSHClient ssh = new SSHClient();
        Session session = null;
        try {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect("192.168.178.221");
            ssh.authPassword("esteban", "yph(h8&L");
            session = ssh.startSession();
            System.out.println("session started");
            final Session.Command cmd = session.exec("cd Paxos; pwd");
            System.out.println(IOUtils.readFully(cmd.getInputStream()).toString());
            cmd.join(1, TimeUnit.SECONDS);
            System.out.println("\n** exit status: " + cmd.getExitStatus());
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (IOException e) {
                // Do Nothing
            }

            ssh.disconnect();
        }
    }

    private static String getUsername() {
        System.out.println("SSH username for Largetable clients:");
        Scanner sc = new Scanner(System.in);
        if (sc.hasNextLine())
            return sc.nextLine();
        return null;
    }

    private static String getPassword() {
        System.out.println("SSH password for Largetable clients:");
        Console console = System.console();
        if (console == null) {
            Scanner sc = new Scanner(System.in);
            String password = null;
            if (sc.hasNextLine()) {
                password = sc.nextLine();
            }
            return password;
        } else {
            return new String(console.readPassword());
        }
    }
}
