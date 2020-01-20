package apps.test;

import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;

public class GUI extends Frame {

    public static void main(String... args) throws IOException {
        new GUI();
    }

    private GUI() throws IOException {
        setSize(300,300);
        setVisible(true);
        setLayout(null);
        addWindowListener(new WindowAdapter(){
            public void windowClosing(WindowEvent e) {
                dispose();
            }
        });
        add(new GUIClientPanel(new TesterClient("192.168.178.221", "esteban", "yph(h8&L", "client1")));
    }
}
