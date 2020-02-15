package apps.test;

public interface ClientUpdateHandler {
    void error(String clientId, int commandNumber, String key, String expected, String actual, String cmdType, String cmdVal);
    void cmdFinished(String clientId, int commandNumber, String key, String value, String cmdType, String cmdData);
}
