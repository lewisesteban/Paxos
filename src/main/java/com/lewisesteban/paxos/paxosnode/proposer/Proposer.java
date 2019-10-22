package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.Logger;
import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.listener.Listener;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.io.IOException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Proposer implements PaxosProposer {

    private MembershipGetter memberList;
    private ProposalFactory propFac;
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Listener listener;
    private Random random = new Random();
    private RunningProposalManager runningProposalManager;
    private AtomicLong lastGeneratedInstanceId = new AtomicLong(-1);

    public Proposer(MembershipGetter memberList, Listener listener, StorageUnit storage, RunningProposalManager runningProposalManager) throws StorageException {
        this.memberList = memberList;
        this.propFac = new ProposalFactory(memberList.getMyNodeId(), storage);
        this.listener = listener;
        this.runningProposalManager = runningProposalManager;
    }

    public long getNewInstanceId() {
        if (listener.getLastInstanceId() > lastGeneratedInstanceId.get()) {
            lastGeneratedInstanceId.set(listener.getLastInstanceId());
        }
        return lastGeneratedInstanceId.incrementAndGet();
    }

    @Override
    public Result propose(Command command, long instanceId) throws StorageException {
        return propose(command, instanceId, false);
    }

    Result propose(Command command, long instanceId, boolean alreadyStartedInManager) throws StorageException {
        if (!alreadyStartedInManager) {
            try {
                runningProposalManager.startProposal(instanceId);
            } catch (RunningProposalManager.InstanceAlreadyRunningException e) {
                listener.waitForConsensusOn(instanceId);
                Listener.ExecutedCommand consensusCmd = listener.tryGetExecutedCommand(instanceId);
                if (consensusCmd.getCommand().equals(command))
                    return new Result(Result.CONSENSUS_ON_THIS_CMD, instanceId, consensusCmd.getResult());
                else
                    return new Result(Result.CONSENSUS_ON_ANOTHER_CMD, instanceId, consensusCmd.getResult());
            }
        }
        try {
            return propose(command, instanceId, 0);
        } finally {
            runningProposalManager.paxosFinished(instanceId);
        }
    }

    private Result tryAgain(Command command, long instanceId, int attempt, boolean badNetworkState) throws StorageException {
        Logger.println("tryAgain node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + command +  " attempt=" + attempt + " badnetworkState=" + badNetworkState);
        if (badNetworkState) {
            if (attempt < 3) {
                return propose(command, instanceId, attempt + 1);
            } else {
                Logger.println("network_error node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + command +  " attempt=" + attempt);
                return new Result(Result.NETWORK_ERROR);
            }
        } else {
            try {
                Thread.sleep(random.nextInt(20 * (attempt + 1)));
            } catch (InterruptedException ignored) { }
            return propose(command, instanceId, attempt + 1);
        }
    }

    private Result propose(Command command, long instanceId, int attempt) throws StorageException {
        Logger.println("propose node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + command +  " attempt=" + attempt);

        Listener.ExecutedCommand thisInstanceExecutedCmd = listener.tryGetExecutedCommand(instanceId);
        if (thisInstanceExecutedCmd != null) {
            boolean sameCmd = (thisInstanceExecutedCmd.getCommand().equals(command));
            return new Result(sameCmd ? Result.CONSENSUS_ON_THIS_CMD : Result.CONSENSUS_ON_ANOTHER_CMD,
                    instanceId, thisInstanceExecutedCmd.getResult());
        }

        // prepare
        Proposal originalProposal = propFac.make(command);
        Proposal prepared;
        try {
            prepared = prepare(instanceId, originalProposal);
        } catch (IOException e) {
            return tryAgain(command, instanceId, attempt, true);
        }
        if (prepared == null) {
            return tryAgain(command, instanceId, attempt, false);
        }
        Logger.println("prepared node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + prepared.getCommand() +  " attempt=" + attempt);

        // accept
        boolean proposalChanged = !originalProposal.getCommand().equals(prepared.getCommand());
        boolean success;
        try {
            success = accept(instanceId, prepared);
        } catch (IOException e) {
            return tryAgain(command, instanceId, attempt, true);
        }
        if (!success) {
            return tryAgain(command, instanceId, attempt, false);
        }
        Logger.println("accepted node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + prepared.getCommand() +  " changed=" + proposalChanged);

        // scatter and return
        scatter(instanceId, prepared);
        java.io.Serializable returnData;
        try {
            returnData = listener.getReturnOf(instanceId, prepared.getCommand());
        } catch (IOException e) {
            return tryAgain(command, instanceId, attempt, true);
        }
        byte resultStatus = proposalChanged ? Result.CONSENSUS_ON_ANOTHER_CMD : Result.CONSENSUS_ON_THIS_CMD;
        return new Result(resultStatus, instanceId, returnData);
    }

    private Proposal prepare(long instanceId, Proposal proposal) throws IOException {
        final AtomicInteger nbOk = new AtomicInteger(0);
        final AtomicInteger nbFailed = new AtomicInteger(0);
        final Queue<Proposal> alreadyAcceptedProps = new ConcurrentLinkedQueue<>();
        final Semaphore anyThread = new Semaphore(0);
        for (RemotePaxosNode node : memberList.getMembers()) {
            executor.submit(() -> {
                try {
                    PrepareAnswer answer = node.getAcceptor().reqPrepare(instanceId, proposal.getId());
                    if (answer.isPrepareOK()) {
                        nbOk.getAndIncrement();
                        if (answer.getAlreadyAccepted() != null) {
                            alreadyAcceptedProps.add(answer.getAlreadyAccepted());
                        }
                    } else {
                        // Someone else is proposing
                        anyThread.release(memberList.getNbMembers());
                    }
                } catch (IOException ignored) {
                    nbFailed.incrementAndGet();
                } finally {
                    anyThread.release();
                }
            });
        }

        int runningThreads = memberList.getNbMembers();
        while (nbOk.get() <= memberList.getNbMembers() / 2 && runningThreads > 0) {
            try {
                anyThread.acquire();
                runningThreads--;
            } catch (InterruptedException ignored) { }
        }

        if (nbOk.get() > memberList.getNbMembers() / 2) {
            return getNewProp(alreadyAcceptedProps, proposal);
        }
        if (nbFailed.get() >= memberList.getNbMembers() / 2) {
            throw new IOException("bad network state");
        }
        return null;
    }

    private Proposal getNewProp(final Queue<Proposal> alreadyAcceptedProps, Proposal originalProp) {
        if (!alreadyAcceptedProps.isEmpty()) {
            Proposal highestIdProp = null;
            for (Proposal prop : alreadyAcceptedProps) {
                if (highestIdProp == null || prop.getId().isGreaterThan(highestIdProp.getId())) {
                    highestIdProp = prop;
                }
            }
            return new Proposal(highestIdProp.getCommand(), originalProp.getId());
        } else {
            return originalProp;
        }
    }

    private boolean accept(long instanceId, Proposal proposal) throws IOException {
        final AtomicInteger nbOk = new AtomicInteger(0);
        final AtomicInteger nbFailed = new AtomicInteger(0);
        final Semaphore anyThread = new Semaphore(0);
        for (RemotePaxosNode node : memberList.getMembers()) {
            executor.submit(() -> {
                try {
                    if (node.getAcceptor().reqAccept(instanceId, proposal)) {
                        nbOk.incrementAndGet();
                    }
                } catch (IOException ignored) {
                    nbFailed.incrementAndGet();
                } finally {
                    anyThread.release();
                }
            });
        }

        int runningThreads = memberList.getNbMembers();
        while (nbOk.get() <= memberList.getNbMembers() / 2 && runningThreads > 0) {
            try {
                anyThread.acquire();
                runningThreads--;
            } catch (InterruptedException ignored) { }
        }
        if (nbOk.get() > memberList.getNbMembers() / 2)
            return true;
        if (nbFailed.get() >= memberList.getNbMembers() / 2)
            throw new IOException("bad network state");
        return false;
    }

    private void scatter(long instanceId, Proposal prepared) {
        listener.execute(instanceId, prepared.getCommand());
        for (RemotePaxosNode node : memberList.getMembers()) {
            if (node.getId() != memberList.getMyNodeId()) {
                executor.submit(() -> {
                    try {
                        node.getListener().execute(instanceId, prepared.getCommand());
                    } catch (IOException ignored) { }
                });
            }
        }
    }
}
