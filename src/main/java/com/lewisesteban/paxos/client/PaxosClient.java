package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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
     * Call this method after creating the client instance in order to guarantee that any ongoing command (started
     * before the client crashed for instance) finishes, before starting any new command.
     *
     * @throws StorageException There is a problem with stable storage. Please fix it and try again.
     */
    public void recover() throws StorageException {
        FailureManager.ClientOperation failedOp = failureManager.getFailedOperation();
        if (failedOp != null) {
            doInstanceRecory(failedOp.getCmdData(), failedOp.getInst(), failedOp.getKeyHash());
        }
    }

    /**
     * Call this method after creating the client instance in order to guarantee that any ongoing command (started
     * before the client crashed for instance) finishes, before starting any new command.
     * Call this method until it succeeds (doesn't throw).
     *
     * @throws StorageException There is a problem with stable storage. Please fix it and try again.
     */
    public void tryRecover() throws CommandException, StorageException {
        FailureManager.ClientOperation failedOp = failureManager.getFailedOperation();
        if (failedOp != null) {
            tryInstanceRecorey(failedOp.getCmdData(), failedOp.getInst(), failedOp.getKeyHash());
        }
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     *
     * @param keyHash Hash of the key corresponding to the command. Used for fragmenting.
     */
    public Serializable tryCommand(Serializable commandData, int keyHash) throws CommandException {
        failureManager.startCommand(commandData, keyHash);
        return fragments.get(keyHash % nbFragments).tryCommand(commandData);
    }

    /**
     * Sends a command and tries again until it succeeds.
     *
     * @param keyHash Hash of the key corresponding to the command. Used for fragmenting.
     */
    public Serializable doCommand(Serializable commandData, int keyHash) {
        failureManager.startCommand(commandData, keyHash);
        return fragments.get(keyHash % nbFragments).doCommand(commandData);
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     * Call this method only to try a failed command again, using the instance returned in the CommandException.
     *
     * @param paxosInstance Paxos instance on which to try the command. Should only be used to resume a failed command.
     * @param keyHash Hash of the key corresponding to the command. Used for fragmenting.
     */
    public Serializable tryCommand(Serializable commandData, Long paxosInstance, int keyHash) throws CommandException {
        return fragments.get(keyHash % nbFragments).tryCommand(commandData, paxosInstance);
    }

    private void doInstanceRecory(Serializable commandData, Long paxosInstance, int keyHash) {
        fragments.get(keyHash % nbFragments).finishInstance(commandData, paxosInstance);
    }

    private void tryInstanceRecorey(Serializable commandData, Long paxosInstance, int keyHash) throws CommandException {
        fragments.get(keyHash % nbFragments).tryFinishInstance(commandData, paxosInstance);
    }
}