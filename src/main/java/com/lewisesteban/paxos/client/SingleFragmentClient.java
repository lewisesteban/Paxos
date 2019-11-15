package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;
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
    private String clientId;
    private ClientCommandSender sender;
    private PaxosProposer dedicatedProposer = null;
    private List<PaxosProposer> nodesTried = new ArrayList<>();
    private Random random = new Random();
    private List<PaxosProposer> nonEndedServers = new ArrayList<>();

    public SingleFragmentClient(List<PaxosProposer> fragmentNodes, String clientId, FailureManager failureManager) {
        this.nodes = fragmentNodes;
        this.commandFactory = new Command.Factory(clientId);
        this.sender = new ClientCommandSender(failureManager);
        this.clientId = clientId;
    }

    /**
     * Sends a command and tries again until it succeeds.
     */
    public Serializable doCommand(Serializable commandData) {
        try {
            return doCommand(commandData, true, null, null);
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
        return doCommand(commandData, false, null, null);
    }

    /**
     * Sends a command and tries again until it succeeds.
     *
     * @param paxosInstance Paxos instance on which to try the command. Should only be used to resume a failed command.
     */
    public Serializable doCommand(Serializable commandData, long cmdNb, Long paxosInstance) {
        try {
            return doCommand(commandData, true, paxosInstance, cmdNb);
        } catch (CommandException e) {
            // should not happen
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     *
     * @param paxosInstance Paxos instance on which to try the command. Should only be used to resume a failed command.
     */
    public Serializable tryCommand(Serializable commandData, long cmdNb, Long paxosInstance) throws CommandException {
        return doCommand(commandData, false, paxosInstance, cmdNb);
    }

    private Serializable doCommand(Serializable commandData, boolean repeatUntilSuccess, Long instance, Long commandNb) throws CommandException {
        tryEndAll(dedicatedProposer);
        Command command;
        if (commandNb == null)
            command = commandFactory.make(commandData);
        else
            command = commandFactory.make(commandData, commandNb);
        nodesTried.clear();
        PaxosProposer node = getPaxosNode(false);
        if (instance == null)
            instance = -1L;
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
                node = changeProposer(nodes.get(e.getDedicatedProposerId()));
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
            return changeProposer(nodes.get(random.nextInt(nodes.size())));
        }
        if (change) {
            dedicatedProposer = null;
            for (PaxosProposer node : nodes) {
                if (!nodesTried.contains(node)) {
                    nodesTried.add(node);
                    return changeProposer(node);
                }
            }
            nodesTried.clear();
            return null;
        } else {
            return dedicatedProposer;
        }
    }

    private PaxosProposer changeProposer(PaxosProposer newProposer) {
        if (dedicatedProposer != null)
            tryEndServer(dedicatedProposer);
        dedicatedProposer = newProposer;
        nonEndedServers.add(newProposer);
        return newProposer;
    }

    private boolean tryEndServer(PaxosProposer server) {
        if (nonEndedServers.contains(server)) {
            try {
                server.endClient(clientId);
                nonEndedServers.remove(server);
                return true;
            } catch (IOException e) {
                return false;
            }
        } else {
            return true;
        }
    }

    private boolean tryEndAll(PaxosProposer exceptThisOne) {
        if (nonEndedServers.isEmpty() || (nonEndedServers.size() == 1 && nonEndedServers.get(0).equals(exceptThisOne)))
            return true;
        boolean success = true;
        for (PaxosProposer proposer : nodes) {
            if (proposer != exceptThisOne) {
                if (!tryEndServer(proposer))
                    success = false;
            }
        }
        return success;
    }

    boolean endClient() {
        return tryEndAll(null);
    }
}