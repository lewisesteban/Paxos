package apps.test;

public class TargetClient implements Target {
    private TesterClient client;
    private GUIClientPanel panel;

    TargetClient(TesterClient client, GUIClientPanel panel) {
        this.client = client;
        this.panel = panel;
    }

    @Override
    public boolean isAlive() {
        return client.isUp();
    }

    @Override
    public boolean kill() {
        if (client.isUp()) {
            return panel.onOffBtnClick();
        }
        return true;
    }

    @Override
    public boolean restore() {
        if (!client.isUp()) {
            return panel.onOffBtnClick();
        }
        return true;
    }
}
