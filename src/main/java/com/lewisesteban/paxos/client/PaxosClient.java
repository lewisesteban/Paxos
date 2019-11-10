package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PaxosClient<NODE extends RemotePaxosNode & PaxosProposer> {
    private List<SingleFragmentClient> fragments = new ArrayList<>();
    private int nbFragments;

    public PaxosClient(List<NODE> allNodes, String clientId) {
        //noinspection OptionalGetWithoutIsPresent
        nbFragments = allNodes.stream()
                .mapToInt(RemotePaxosNode::getFragmentId).max().getAsInt() + 1;
        allNodes.stream()
                .collect(Collectors.groupingBy(RemotePaxosNode::getFragmentId))
                .forEach((fragmentNb, fragmentNodes) -> fragments.add(new SingleFragmentClient(fragmentNodes.stream().map(node -> (PaxosProposer) node).collect(Collectors.toList()), clientId)));
    }

    /**
     * Sends a command and tries again until it succeeds.
     *
     * @param keyHash Hash of the key corresponding to the command. Used for fragmenting.
     */
    public Serializable tryCommand(Serializable commandData, int keyHash) throws CommandException {
        return fragments.get(keyHash % nbFragments).tryCommand(commandData);
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     *
     * @param keyHash Hash of the key corresponding to the command. Used for fragmenting.
     */
    public Serializable doCommand(Serializable commandData, int keyHash) {
        return fragments.get(keyHash % nbFragments).doCommand(commandData);
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     *
     * @param paxosInstance Paxos instance on which to try the command. Should only be used to resume a failed command.
     * @param keyHash Hash of the key corresponding to the command. Used for fragmenting.
     */
    public Serializable tryCommand(Serializable commandData, Long paxosInstance, int keyHash) throws CommandException {
        return fragments.get(keyHash % nbFragments).tryCommand(commandData, paxosInstance);
    }
}