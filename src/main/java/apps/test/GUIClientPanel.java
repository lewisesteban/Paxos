package apps.test;

import java.awt.*;
import java.awt.event.ActionEvent;

class GUIClientPanel extends Panel implements ClientUpdateHandler {
    private TesterClient client;
    private Label cmdsLbl;
    private Label onOffLbl;
    private Button onOffBtn;

    GUIClientPanel(TesterClient client, int index) {
        this.client = client;
        client.setClientUpdateHandler(this);
        setBounds(0, 40 + (index * 35), 250, 30);

        Label clientIdLbl = new Label(client.getClientId());
        add(clientIdLbl);

        cmdsLbl = new Label("000000");
        add(cmdsLbl);

        onOffLbl = new Label("OFF");
        onOffLbl.setForeground(Color.WHITE);
        onOffLbl.setBackground(Color.RED);
        add(onOffLbl);

        onOffBtn = new Button("Turn on");
        onOffBtn.addActionListener(this::onOffBtnClick);
        add(onOffBtn);
    }

    @SuppressWarnings("unused")
    private void onOffBtnClick(ActionEvent event) {
        if (client.isUp()) {
            if (client.kill()) {
                onOffLbl.setText("OFF");
                onOffLbl.setBackground(Color.RED);
                onOffLbl.setForeground(Color.WHITE);
                onOffBtn.setLabel("Turn on");
            }
        } else {
            if (client.launch()) {
                client.startTesting();
                onOffLbl.setText("ON");
                onOffLbl.setBackground(Color.GREEN);
                onOffLbl.setForeground(Color.BLACK);
                onOffBtn.setLabel("Turn off");
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
}
