package apps;

import com.lewisesteban.paxos.storage.StorageException;
import largetable.Client;
import largetable.LargeTableClient;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Interpreter {
    // regex: ("(([^\\\"]*\\(\\\\)*")|([^\"\\])|(\\\\))*")|(\S+)
    private static final String regex = "(\"(([^\\\\\"]*\\\\(\\\\\\\\)*\")|([^\"\\\\])|(\\\\\\\\))*\")|(\\S+)";
    private Pattern pattern = Pattern.compile(regex);
    private Map<String, Command> commands = new TreeMap<>();
    private Client client;

    Interpreter(largetable.Client client) {
        this.client = client;
        commands.put("LAST", this::cmdLast);
        commands.put("GET", this::cmdGet);
        commands.put("PUT", this::cmdPut);
        commands.put("APPEND", this::cmdAppend);
        commands.put("END", this::cmdEnd);
        commands.put("EXIT", this::cmdExit);
    }

    String interpret(String line) {

        String[] words = splitLine(line);
        if (words == null || words.length == 0 || !commands.containsKey(words[0].toUpperCase())) {
            return "ERR invalid command. Available commands: LAST | GET | PUT | APPEND | END | EXIT";
        }
        String cmd = words[0].toUpperCase();
        String[] args = Arrays.copyOfRange(words, 1, words.length);
        try {
            if (cmd.toUpperCase().equals("EXIT"))
                return "EXIT";
            String res = commands.get(cmd).execute(args);
            if (res == null) {
                return "OK";
            } else {
                return "OK " + res;
            }
        } catch (StorageException e) {
            return "ERR storage. " + e.getMessage();
        } catch (LargeTableClient.LargeTableException e) {
            return "ERR network. " + e.getMessage();
        } catch (InvalidCommandException e) {
            return "ERR invalid command. " + e.getMessage();
        }
    }

    private String[] splitLine(String line) {

        List<String> words = new ArrayList<>();
        Matcher m = pattern.matcher(line);
        while (m.find()) {
            if (m.group(1) != null) {
                words.add(unBox(m.group(1)));
            } else {
                words.add(m.group(7));
            }
        }
        String[] array = new String[words.size()];
        for (int i = 0; i < array.length; ++i)
            array[i] = words.get(i);
        return array;
    }

    private String cmdLast(String[] args) throws InvalidCommandException, Client.LargeTableException, StorageException {
        if (args.length != 1)
            throw new InvalidCommandException("Expected: \"LAST RESULT\" or \"LAST COMMAND\"");
        String arg = args[0].toUpperCase();
        if (!arg.equals("RESULT") && !arg.equals("COMMAND"))
            throw new InvalidCommandException("Expected: \"LAST RESULT\" or \"LAST COMMAND\"");
        Client.ExecutedCommand executedCommand = client.recover();
        if (executedCommand != null) {
            String cmd = "";
            if (arg.equals("COMMAND")) {
                switch (executedCommand.getCmdType()) {
                    case Client.ExecutedCommand.TYPE_GET:
                        cmd = "GET " + boxUp(executedCommand.getCmdKey());
                        break;
                    case Client.ExecutedCommand.TYPE_APPEND:
                        cmd = "APPEND " + boxUp(executedCommand.getCmdKey()) + " " + boxUp(executedCommand.getCmdValue());
                        break;
                    case Client.ExecutedCommand.TYPE_PUT:
                        cmd = "PUT " + boxUp(executedCommand.getCmdKey()) + " " + boxUp(executedCommand.getCmdValue());
                        break;
                }
                return cmd;
            } else {
                return executedCommand.getResult();
            }
        } else {
            return "";
        }
    }

    private String cmdGet(String[] args) throws InvalidCommandException, Client.LargeTableException {
        if (args.length != 1) {
            throw new InvalidCommandException("Expected: GET [key]");
        }
        return client.get(args[0]);
    }

    private String cmdPut(String[] args) throws InvalidCommandException, Client.LargeTableException {
        if (args.length != 2) {
            throw new InvalidCommandException("Expected: PUT [key] [value]");
        }
        client.put(args[0], args[1]);
        return null;
    }

    private String cmdAppend(String[] args) throws InvalidCommandException, Client.LargeTableException {
        if (args.length != 2) {
            throw new InvalidCommandException("Expected: PUT [key] [value]");
        }
        client.append(args[0], args[1]);
        return null;
    }

    @SuppressWarnings("unused")
    private String cmdEnd(String[] args) {
        client.end();
        return null;
    }

    @SuppressWarnings("unused")
    private String cmdExit(String[] args) {
        return "EXIT";
    }

    private String boxUp(String str) {
        return "\"" + str.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private String unBox(String str) {
        return str.substring(1, str.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
    }

    interface Command {
        String execute(String[] args) throws StorageException, LargeTableClient.LargeTableException, InvalidCommandException;
    }

    private class InvalidCommandException extends Exception {
        InvalidCommandException(String msg) {
            super(msg);
        }
    }
}
