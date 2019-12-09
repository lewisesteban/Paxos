package largetable;

import com.lewisesteban.paxos.client.CommandException;
import com.lewisesteban.paxos.client.PaxosClient;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.StorageException;

import java.io.Serializable;
import java.util.List;

/**
 * Client that will throw exceptions if there is a problem with the Paxos network.
 * You must call recover after creating the client instance and before doing any other request.
 * When a method throws LargeTableException, you can call tryAgain to try again.
 * Don't forget to end() the client.
 */
public class LargeTableClient<NODE extends PaxosProposer & RemotePaxosNode> implements Client {
    private PaxosClient<NODE> paxosClient;
    private int lastCommandHash = 0;
    private CommandException lastCommandException = null;

    public LargeTableClient(List<NODE> allNodes, String clientId, FileAccessorCreator fileAccessorCreator) throws StorageException {
        paxosClient = new PaxosClient<>(allNodes, clientId,
                (name, dir) -> new SafeSingleFileStorage(name, dir, fileAccessorCreator));
    }

    /**
     * Attemps to finish the last command that was started but not finished, if any.
     *
     * @return In any case, this method will return the last executed command, as well as its result.
     * @throws StorageException There is a problem with stable storage access.
     * @throws LargeTableException There is a problem with the Paxos network.
     */
    public ExecutedCommand recover() throws StorageException, LargeTableException {
        try {
            com.lewisesteban.paxos.client.ExecutedCommand paxosExecutedCmd = paxosClient.tryRecover();
            if (paxosExecutedCmd == null)
                return null;
            return new ExecutedCommand((Command)paxosExecutedCmd.getCommandData(), paxosExecutedCmd.getReturnedData());
        } catch (CommandException e) {
            lastCommandException = e;
            lastCommandHash = ((Command)(e.getCommandData())).getData()[0].hashCode();
            throw new LargeTableException(e);
        }
    }

    public String get(String key) throws LargeTableException {
        return tryCommand(new Command(Command.GET, new String[] { key }));
    }

    public void put(String key, String value) throws LargeTableException {
        tryCommand(new Command(Command.PUT, new String[] { key, value }));
    }

    public void append(String key, String value) throws LargeTableException {
        tryCommand(new Command(Command.APPEND, new String[] { key, value }));
    }

    /**
     * Attempts to try the last command again.
     *
     * @throws LargeTableException There is a problem with the Paxos network.
     */
    public String tryAgain() throws LargeTableException {
        try {
            Serializable res = paxosClient.tryCommandAgain(lastCommandException, lastCommandHash);
            if (res == null)
                return null;
            return (String)res;
        } catch (CommandException e) {
            lastCommandException = e;
            throw new LargeTableException(e);
        }
    }

    /**
     * Call this whenever the client stops sending requests, even temporarily.
     */
    public void end() {
        paxosClient.end();
    }

    private String tryCommand(Command command) throws LargeTableException {
        try {
            return (String) paxosClient.tryCommand(command, command.getData()[0].hashCode());
        } catch (CommandException e) {
            this.lastCommandException = e;
            this.lastCommandHash = command.getData()[0].hashCode();
            throw new LargeTableException(e);
        }
    }
}
