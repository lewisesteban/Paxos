package apps.test;

import java.awt.*;

class GUIClientPanel extends Panel implements ClientUpdateHandler {
    private TesterClient client;
    private Label cmdsLbl;
    private Label onOffLbl;
    private Button onOffBtn;

    GUIClientPanel(TesterClient client, int index, int xOffset, int yOffset) {
        this.client = client;
        client.setClientUpdateHandler(this);
        setBounds(xOffset, yOffset + (index * 35), 250, 30);

        Label clientIdLbl = new Label(client.getClientId());
        add(clientIdLbl);

        cmdsLbl = new Label("000000");
        add(cmdsLbl);

        onOffLbl = new Label("OFF");
        onOffLbl.setForeground(Color.WHITE);
        onOffLbl.setBackground(Color.RED);
        add(onOffLbl);

        onOffBtn = new Button("Turn on");
        onOffBtn.addActionListener(event -> onOffBtnClick());
        add(onOffBtn);
    }

    boolean onOffBtnClick() {
        if (client.isUp()) {
            if (client.kill()) {
                setAlive(false);
                return true;
            } else {
                return false;
            }
        } else {
            if (client.launch()) {
                client.startTesting();
                setAlive(true);
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public void error(String clientId, int commandNumber, String key, String expected, String actual, String cmdType, String cmdVal) {
        // TODO
    }

    @Override
    public void cmdFinished(String clientId, int commandNumber, String key, String value, String cmdType, String cmdData) {
        cmdsLbl.setText(Integer.toString(commandNumber));
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
