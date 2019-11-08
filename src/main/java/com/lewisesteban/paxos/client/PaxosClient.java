package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Does not handle fragmentation and dedicated proposer redirection
 */
public class PaxosClient {

    private List<PaxosProposer> nodes;
    private Command.Factory commandFactory;
    private ClientCommandSender sender;
    private Integer dedicatedProposer = null;
    private List<PaxosProposer> nodesTried = new ArrayList<>();
    private Random random = new Random();

    public PaxosClient(List<PaxosProposer> nodes, String clientId) {
        this.nodes = nodes;
        this.commandFactory = new Command.Factory(clientId);
        this.sender = new ClientCommandSender();
    }

    /**
     * Sends a command and tries again until it succeeds.
     */
    public Serializable doCommand(Serializable commandData) {
        try {
            return doCommand(commandData, true);
        } catch (ClientCommandSender.CommandException e) {
            // should not happen
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     */
    public Serializable tryCommand(Serializable commandData) throws ClientCommandSender.CommandException {
        return doCommand(commandData, false);
    }

    private Serializable doCommand(Serializable commandData, boolean repeatUntilSuccess) throws ClientCommandSender.CommandException {
        Command command = commandFactory.make(commandData);
        nodesTried.clear();
        PaxosProposer node = getPaxosNode(false);
        long instance = -1;
        while (true) {
            try {
                if (instance == -1) {
                    return sender.doCommand(node, command);
                } else {
                    return sender.doCommand(node, command, instance);
                }
            } catch (ClientCommandSender.DedicatedProposerRedirection e) {
                if (e.getInstanceThatMayHaveBeenInitiated() != null)
                    instance = e.getInstanceThatMayHaveBeenInitiated();
                dedicatedProposer = e.getDedicatedProposerId();
                node = getPaxosNode(false);
            } catch (ClientCommandSender.CommandException e) {
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