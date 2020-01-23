package apps.test;

import java.awt.*;
import java.awt.event.ActionEvent;

class GUIServerPanel extends Panel {
    private TesterServer server;
    private Label onOffLbl;
    private Button onOffBtn;

    GUIServerPanel(TesterServer server, int GUIIndex) {
        this.server = server;
        setBounds(250, 40 + (GUIIndex * 35), 250, 30);

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
        onOffBtn.addActionListener(this::onOffBtnClick);
        add(onOffBtn);
    }

    @SuppressWarnings("unused")
    private void onOffBtnClick(ActionEvent event) {
        if (server.isUp()) {
            if (server.kill()) {
                onOffLbl.setText("OFF");
                onOffLbl.setBackground(Color.RED);
                onOffLbl.setForeground(Color.WHITE);
                onOffBtn.setLabel("Turn on");
            }
        } else {
            if (server.launch() && server.connect()) {
                onOffLbl.setText("ON");
                onOffLbl.setBackground(Color.GREEN);
                onOffLbl.setForeground(Color.BLACK);
                onOffBtn.setLabel("Turn off");
            }
        }
    }
}
