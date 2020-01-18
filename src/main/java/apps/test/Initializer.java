package apps.test;

import java.io.Console;
import java.io.IOException;
import java.util.Scanner;

public class Initializer {

    public static void main(String... args) throws IOException, InterruptedException {

        String username = getUsername(); // username is esteban
        if (username == null)
            return;
        String password = getPassword();
        LargetableTester tester = new LargetableTester(username, password);
        tester.start();
        System.exit(0);
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
