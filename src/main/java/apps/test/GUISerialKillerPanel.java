package apps.test;


import javax.swing.*;
import java.awt.*;
import java.text.NumberFormat;

class GUISerialKillerPanel extends Panel {
    private SerialKiller serialKiller;
    private Button onOffBtn;
    private Label onOffLbl;
    private JFormattedTextField minWait, maxWait;

    GUISerialKillerPanel(java.util.List<Target> targets, int xOffset, int yOffset) {
        serialKiller = new SerialKiller(targets);
        setBounds(xOffset, yOffset, 250, 60);

        add(new Label("Auto kill/restore"));

        onOffLbl = new Label("OFF");
        onOffLbl.setForeground(Color.WHITE);
        onOffLbl.setBackground(Color.RED);
        add(onOffLbl);

        onOffBtn = new Button("Turn on");
        onOffBtn.addActionListener(event -> onOffBtnClick());
        add(onOffBtn);

        Panel confPanel = new Panel();
        confPanel.add(new Label("Wait (ms):"));
        minWait = new JFormattedTextField(NumberFormat.getIntegerInstance());
        minWait.setValue(0L);
        minWait.setColumns(4);
        confPanel.add(minWait);
        confPanel.add(new Label("to"));
        maxWait = new JFormattedTextField(NumberFormat.getIntegerInstance());
        maxWait.setValue(500L);
        maxWait.setColumns(4);
        confPanel.add(maxWait);
        add(confPanel);
    }

    synchronized void turnOff() {
        if (serialKiller.isGoing()) {
            onOffBtnClick();
        }
    }

    private synchronized void onOffBtnClick() {
        if (serialKiller.isGoing()) {
            serialKiller.stop();
            setAlive(false);
        } else {
            try {
                serialKiller.startInBackground((Integer) minWait.getValue(), (Integer) maxWait.getValue());
            } catch (ClassCastException e) {
                serialKiller.startInBackground(((Long) minWait.getValue()).intValue(), ((Long) maxWait.getValue()).intValue());
            }
            setAlive(true);
        }
    }

    private void setAlive(boolean alive) {
        if (alive) {
            onOffLbl.setText("ON");
            onOffLbl.setBackground(Color.GREEN);
            onOffLbl.setForeground(Color.BLACK);
            onOffBtn.setLabel("Turn off");
        } else {
            onOffLbl.setText("OFF");
            onOffLbl.setBackground(Color.RED);
            onOffLbl.setForeground(Color.WHITE);
            onOffBtn.setLabel("Turn on");
        }
    }
}
