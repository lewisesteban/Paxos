package apps.test;

import java.awt.*;

class GUIServerPanel extends Panel {
    private TesterServer server;
    private Label onOffLbl;
    private Button onOffBtn;

    GUIServerPanel(TesterServer server, int GUIIndex, int xOffset, int yOffset) {
        this.server = server;
        setBounds(xOffset, yOffset + (GUIIndex * 35), 250, 30);

        Label fragmentLbl = new Label(Integer.toString(server.getFragmentId()));
        fragmentLbl.setBackground(Color.BLACK);
        fragmentLbl.setForeground(Color.WHITE);
        add(fragmentLbl);

        Label srvIdLbl = new Label(Integer.toString(server.getNodeId()));
        add(srvIdLbl);

        onOffLbl = new Label();
        onOffLbl.setText("ON");
        onOffLbl.setBackground(Color.GREEN);
        onOffLbl.setForeground(Color.BLACK);
        add(onOffLbl);

        onOffBtn = new Button("Turn off");
        onOffBtn.addActionListener(event -> onOffBtnClick());
        add(onOffBtn);
    }

    boolean onOffBtnClick() {
        if (server.isUp()) {
            if (server.kill()) {
                setAlive(false);
                return true;
            } else {
                return false;
            }
        } else {
            if (server.launch()) {
                setAlive(true);
                return true;
            } else {
                return false;
            }
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
