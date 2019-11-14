package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Client for a single fragment
 */
@SuppressWarnings("WeakerAccess")
public class SingleFragmentClient {

    private List<PaxosProposer> nodes;
    private Command.Factory commandFactory;
    private ClientCommandSender sender;
    private Integer dedicatedProposer = null;
    private List<PaxosProposer> nodesTried = new ArrayList<>();
    private Random random = new Random();

    public SingleFragmentClient(List<PaxosProposer> fragmentNodes, String clientId, FailureManager failureManager) {
        this.nodes = fragmentNodes;
        this.commandFactory = new Command.Factory(clientId);
        this.sender = new ClientCommandSender(failureManager);
    }

    public Serializable finishInstance(Serializable commandData, long instance) {
        try {
            return doCommand(commandData, true, instance, true);
        } catch (CommandException e) {
            // should not happen
            e.printStackTrace();
            return null;
        }
    }

    public Serializable tryFinishInstance(Serializable commandData, long instance) throws CommandException {
        return doCommand(commandData, false, instance, true);
    }

    /**
     * Sends a command and tries again until it succeeds.
     */
    public Serializable doCommand(Serializable commandData) {
        try {
            return doCommand(commandData, true, null, false);
        } catch (CommandException e) {
            // should not happen
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     */
    public Serializable tryCommand(Serializable commandData) throws CommandException {
        return doCommand(commandData, false, null, false);
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     *
     * @param paxosInstance Paxos instance on which to try the command. Should only be used to resume a failed command.
     */
    public Serializable tryCommand(Serializable commandData, Long paxosInstance) throws CommandException {
        return doCommand(commandData, false, paxosInstance, false);
    }

    private Serializable doCommand(Serializable commandData, boolean repeatUntilSuccess, Long instance, boolean onlyThisInstance) throws CommandException {
        Command command = commandFactory.make(commandData);
        nodesTried.clear();
        PaxosProposer node = getPaxosNode(false);
        if (instance == null)
            instance = -1L;
        while (true) {
            try {
                if (instance == -1) {
                    return sender.doCommand(node, command);
                } else {
                    return sender.doCommand(node, command, instance, onlyThisInstance);
                }
            } catch (ClientCommandSender.DedicatedProposerRedirection e) {
                if (e.getInstanceThatMayHaveBeenInitiated() != null)
                    instance = e.getInstanceThatMayHaveBeenInitiated();
                dedicatedProposer = e.getDedicatedProposerId();
                node = getPaxosNode(false);
            } catch (ClientCommandSender.CommandFailedException e) {
                if (e.getInstanceThatMayHaveBeenInitiated() != null)
                    instance = e.getInstanceThatMayHaveBeenInitiated();
                backOff();
                node = getPaxosNode(true);
                if (node == null) {
                    if (repeatUntilSuccess) {
                        nodesTried.clear();
                        node = getPaxosNode(true);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    private void backOff() {
        if (nodesTried.size() >= 3) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
            }
        }
    }
    
    private PaxosProposer getPaxosNode(boolean change) {
        if (dedicatedProposer == null) {
            dedicatedProposer = random.nextInt(nodes.size());
            return nodes.get(dedicatedProposer);
        }
        if (change) {
            dedicatedProposer = null;
            for (PaxosProposer node : nodes) {
                if (!nodesTried.contains(node)) {
                    nodesTried.add(node);
                    return node;
                }
            }
            nodesTried.clear();
            return null;
        } else {
            return nodes.get(dedicatedProposer);
        }
    }
}