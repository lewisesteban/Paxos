package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A full fail-safe client for Paxos.
 * Methods with names starting with "try" should be used only if you wish to implement failure handling yourself.
 * Otherwise, you should use the other methods, which will try again until they succeed.
 * After creating the client, you should call "recover" or "tryRecover" immediately, before starting any other command.
 */
public class PaxosClient<NODE extends RemotePaxosNode & PaxosProposer> {
    private List<SingleFragmentClient> fragments = new ArrayList<>();
    private int nbFragments;
    private FailureManager failureManager;
    private ExecutorService executorService = Executors.newCachedThreadPool();

    public PaxosClient(List<NODE> allNodes, String clientId, StorageUnit.Creator storage) {
        failureManager = new FailureManager(storage, clientId);
        //noinspection OptionalGetWithoutIsPresent
        nbFragments = allNodes.stream()
                .mapToInt(RemotePaxosNode::getFragmentId).max().getAsInt() + 1;
        allNodes.stream()
                .collect(Collectors.groupingBy(RemotePaxosNode::getFragmentId))
                .forEach((fragmentNb, fragmentNodes) -> fragments.add(new SingleFragmentClient(
                        fragmentNodes.stream().map(node -> (PaxosProposer) node).collect(Collectors.toList()),
                        clientId,
                        failureManager)));
    }

    /**
     * Call this method after creating the client instance in order to guarantee that a command that has been started
     * but not finished finishes, before starting any new command.
     * Returns the last executed command.
     *
     * @throws StorageException There is a problem with stable storage. Please fix it and try again.
     */
    public ExecutedCommand recover() throws StorageException {
        FailureManager.ClientOperation failedOp = failureManager.getLastStartedOperation();
        if (failedOp != null) {
            Serializable result = fragments.get(failedOp.getKeyHash() % nbFragments).doCommand(failedOp.getCmdData(), failedOp.getCmdNb(), failedOp.getInst());
            return new ExecutedCommand(failedOp.getCmdData(), result);
        }
        return null;
    }

    /**
     * Call this method after creating the client instance in order to guarantee that a command that has been started
     * but not finished finishes, before starting any new command.
     * Call this method until it succeeds (doesn't throw).
     * Returns the last executed command.
     *
     * @throws StorageException There is a problem with stable storage. Please fix it and try again.
     */
    public ExecutedCommand tryRecover() throws CommandException, StorageException {
        FailureManager.ClientOperation failedOp = failureManager.getLastStartedOperation();
        if (failedOp != null) {
            Serializable result = fragments.get(failedOp.getKeyHash() % nbFragments).tryCommand(failedOp.getCmdData(), failedOp.getCmdNb(), failedOp.getInst());
            return new ExecutedCommand(failedOp.getCmdData(), result);
        }
        return null;
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, which is needed to try the command again.
     *
     * @param keyHash Hash of the key corresponding to the command. Used for fragmenting.
     */
    public Serializable tryCommand(Serializable commandData, int keyHash) throws CommandException {
        failureManager.setOngoingCmdData(commandData);
        failureManager.setOngoingCmdKeyHash(keyHash);
        return fragments.get(keyHash % nbFragments).tryCommand(commandData);
    }

    /**
     * Sends a command and tries again until it succeeds.
     *
     * @param keyHash Hash of the key corresponding to the command. Used for fragmenting.
     */
    public Serializable doCommand(Serializable commandData, int keyHash) {
        failureManager.setOngoingCmdData(commandData);
        failureManager.setOngoingCmdKeyHash(keyHash);
        return fragments.get(keyHash % nbFragments).doCommand(commandData);
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, which is needed to try the command again.
     * Call this method only to try a failed command again, using the thrown CommandException.
     *
     * @param e The exception thrown during the last attempt to send this command
     * @param keyHash Hash of the key corresponding to the command. Used for fragmenting.
     */
    public Serializable tryCommandAgain(CommandException e, int keyHash) throws CommandException {
        return fragments.get(keyHash % nbFragments).tryCommand(e.getCommandData(), e.getCommandNb(), e.getInstanceThatMayHaveBeenInitiated());
    }

    /**
     * Call this after you stop sending commands, even temporarily, to let Paxos know that you have received the sult of
     * the last command.
     *
     * @return Whether or not the operation was successful.
     */
    public boolean end() {
        AtomicBoolean success = new AtomicBoolean(true);
        for (SingleFragmentClient fragmentClient : fragments) {
            executorService.submit(fragmentClient::endClient);
        }
        try {
            executorService.invokeAll(fragments.stream()
                    .map(fragment -> (Callable<Boolean>) fragment::endClient).collect(Collectors.toList()))
                    .forEach(future -> {
                        try {
                            if (!future.get())
                                success.set(false);
                        } catch (InterruptedException | ExecutionException e) {
                            success.set(false);
                        }
                    });
            return success.get();
        } catch (InterruptedException e) {
            return false;
        }
    }
}