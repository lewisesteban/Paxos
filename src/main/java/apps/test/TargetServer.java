package apps.test;

public class TargetServer implements Target {
    private TesterServer server;
    private GUIServerPanel panel;

    TargetServer(TesterServer server, GUIServerPanel panel) {
        this.server = server;
        this.panel = panel;
    }

    @Override
    public boolean isAlive() {
        return server.isUp();
    }

    @Override
    public boolean kill() {
        if (server.isUp()) {
            return panel.onOffBtnClick();
        }
        return true;
    }

    @Override
    public boolean restore() {
        if (!server.isUp()) {
            return panel.onOffBtnClick();
        }
        return true;
    }
}
