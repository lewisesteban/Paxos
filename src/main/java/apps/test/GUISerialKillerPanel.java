package apps.test;


import java.awt.*;

class GUISerialKillerPanel extends Panel {
    private SerialKiller serialKiller;
    private Button onOffBtn;
    private Label onOffLbl;

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
    }

    private void onOffBtnClick() {
        if (serialKiller.isGoing()) {
            serialKiller.stop();
            setAlive(false);
        } else {
            serialKiller.startInBackground(300, 600);
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
