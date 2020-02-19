package apps.testtools;

import com.lewisesteban.paxos.paxosnode.Command;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AcceptorDataReader {
    private static final String STORAGE_KEY_LAST_ACCEPTED_ID_NODE = "c";
    private static final String STORAGE_KEY_LAST_ACCEPTED_CMD = "e";

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        readStorage();
    }

    private static void readStorage() throws IOException, ClassNotFoundException {
        List<Long> list = new LinkedList<>();
        File dir = new File(".");
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                String name = file.getName();
                if (name.endsWith("_tmp"))
                    name = name.substring(0, name.indexOf("_tmp"));
                long instance = Long.parseLong(name.substring("inst".length()));
                if (!list.contains(instance)) {
                    Map<String, String> content = readFile(file);
                    if (!content.isEmpty()) {
                        if (content.get(STORAGE_KEY_LAST_ACCEPTED_ID_NODE) != null) {
                            Command lastAcceptedCmd;
                            lastAcceptedCmd = deserializeCommand(content.get(STORAGE_KEY_LAST_ACCEPTED_CMD));
                            System.out.println("inst=" + instance
                                    + "\tclient=" + lastAcceptedCmd.getClientId()
                                    + "\tcmdNb=" + lastAcceptedCmd.getClientCmdNb()
                                    + "\tnoOp=" + lastAcceptedCmd.isNoOp()
                                    + "\tcmd=" + (lastAcceptedCmd.isNoOp() ? "N/A" : lastAcceptedCmd.getData()));
                        }
                        list.add(instance);
                    }
                }
            }
        }
    }

    private static Map<String, String> readFile(File file) throws ClassNotFoundException, IOException {
        FileInputStream reader = new FileInputStream(file.getPath());
        ObjectInputStream ois = new ObjectInputStream(reader);
        Object res = ois.readObject();
        //noinspection unchecked
        TreeMap<String, String> content = (TreeMap<String, String>) res;
        ois.close();
        reader.close();
        return content;
    }

    private static Command deserializeCommand(String serialized) throws IOException, ClassNotFoundException {
        byte[] bytes = Base64.decode(serialized);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        return (Command) objectInputStream.readObject();
    }
}
