package largetable;

import com.lewisesteban.paxos.storage.StorageException;

import java.io.Serializable;

public interface Client {

    LargeTableClient.ExecutedCommand recover() throws StorageException, LargeTableException;
    String get(String key) throws LargeTableException;
    void put(String key, String value) throws LargeTableException;
    void append(String key, String value) throws LargeTableException;
    String tryAgain() throws LargeTableException;
    void end();

    class LargeTableException extends Exception {
        LargeTableException(Throwable e) {
            super(e);
        }
    }

    class ExecutedCommand {
        public final static byte TYPE_GET = Command.GET;
        public final static byte TYPE_PUT = Command.PUT;
        public final static byte TYPE_APPEND = Command.APPEND;

        private Command cmd;
        private Serializable result;

        ExecutedCommand(Command cmd, Serializable result) {
            this.cmd = cmd;
            this.result = result;
        }

        public byte getCmdType() {
            return cmd.getType();
        }

        public String getCmdKey() {
            return cmd.getData()[0];
        }

        public String getCmdValue() {
            if (getCmdType() == TYPE_GET)
                return null;
            return cmd.getData()[1];
        }

        public String getResult() {
            if (getCmdType() != TYPE_GET)
                return null;
            return (String)result;
        }
    }
}

