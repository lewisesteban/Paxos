package largetable;

import java.io.Serializable;
import java.util.Map;

public class Command implements Serializable {
    static final byte GET = 0;
    static final byte PUT = 1;
    static final byte APPEND = 2;

    private byte type;
    private String[] data;

    /**
     * @param data The first value of the array must be the key.
     */
    Command(byte type, String[] data) {
        this.type = type;
        this.data = data;
    }

    byte getType() {
        return type;
    }

    String[] getData() {
        return data;
    }

    String apply(Map<String, String> table) {
        switch (type) {
            case GET:
                return table.get(data[0]);
            case PUT:
                table.put(data[0], data[1]);
                break;
            case APPEND:
                table.merge(data[0], data[1], String::concat);
                break;
        }
        return null;
    }

    @Override
    public String toString() {
        return "val" + data[1];
    }
}
