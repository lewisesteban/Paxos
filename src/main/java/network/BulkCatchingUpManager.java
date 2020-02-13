package network;

import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.listener.CatchingUpManager;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;

import java.io.IOException;
import java.util.*;

public class BulkCatchingUpManager implements CatchingUpManager {
    static int TIMEOUT = 10000;

    private AcceptorRPCHandle remoteAcceptorClient;
    private boolean catchingUp = false;
    private long targetInst = -1;
    private long firstWantedInst = -1;
    private boolean preparing = false;

    private Set<Long> finishedInstances = new TreeSet<>();
    private boolean[] expectedInstances = null;
    private int nbExpectedInstances;

    private ArrayList<Request<Proposal.ID, PrepareAnswer>> prepareRequests = null;
    private ArrayList<Request<Proposal, AcceptAnswer>> acceptRequests = null;

    BulkCatchingUpManager(AcceptorRPCHandle remoteAcceptorClient) {
        this.remoteAcceptorClient = remoteAcceptorClient;
    }

    @Override
    public synchronized boolean isCatchingUp() {
        return catchingUp;
    }

    @Override
    public synchronized void startCatchUp(long firstInstInclusive, long lastInstInclusive) {
        targetInst = lastInstInclusive;
        firstWantedInst = firstInstInclusive;
        catchingUp = true;
        preparing = true;
        nbExpectedInstances = (int)(lastInstInclusive - firstInstInclusive + 1);
        finishedInstances.clear();
        prepareRequests = new ArrayList<>(nbExpectedInstances);
        prepareRequests.addAll(Collections.nCopies(nbExpectedInstances, null));
        expectedInstances = new boolean[nbExpectedInstances];
        Arrays.fill(expectedInstances, true);
    }

    private synchronized void switchToAcceptPhase() {
        preparing = false;
        nbExpectedInstances = (int)(targetInst - firstWantedInst + 1);
        acceptRequests = new ArrayList<>(nbExpectedInstances);
        acceptRequests.addAll(Collections.nCopies(nbExpectedInstances, null));
        Arrays.fill(expectedInstances, true);
        for (long inst : finishedInstances) {
            expectedInstances[(int) (inst - firstWantedInst)] = false;
            nbExpectedInstances--;
        }
    }

    private synchronized void finish() {
        catchingUp = false;
    }

    private synchronized void instanceHasArrived(long inst) {
        if (expectedInstances[(int) (inst - firstWantedInst)]) {
            expectedInstances[(int) (inst - firstWantedInst)] = false;
            nbExpectedInstances--;
        }
        if (nbExpectedInstances == 0) {
            if (preparing) {
                doBulkPrepare();
                notifyAll();
                switchToAcceptPhase();
            } else {
                doBulkAccept();
                notifyAll();
                finish();
            }
        }
    }

    private synchronized void doBulkPrepare() {
        long[] instanceIds = new long[prepareRequests.size()];
        Proposal.ID[] propIds = new Proposal.ID[instanceIds.length];
        for (int i = 0; i < instanceIds.length; ++i) {
            if (prepareRequests.get(i) == null) {
                instanceIds[i] = -1;
                propIds[i] = null;
            } else {
                instanceIds[i] = prepareRequests.get(i).inst;
                propIds[i] = prepareRequests.get(i).arg;
            }
        }
        try {
            PrepareAnswer[] answers = remoteAcceptorClient.bulkPrepare(instanceIds, propIds);
            for (int i = 0; i < instanceIds.length; ++i) {
                if (prepareRequests.get(i) != null)
                    prepareRequests.get(i).setResponse(answers[i]);
            }
        } catch (IOException e) {
            for (int i = 0; i < instanceIds.length; ++i) {
                if (prepareRequests.get(i) != null)
                    prepareRequests.get(i).setResponse(e);
            }
        }
    }

    private void doBulkAccept() {
        long[] instanceIds = new long[acceptRequests.size()];
        Proposal[] proposals = new Proposal[instanceIds.length];
        for (int i = 0; i < instanceIds.length; ++i) {
            if (acceptRequests.get(i) == null) {
                instanceIds[i] = -1;
                proposals[i] = null;
            } else {
                instanceIds[i] = acceptRequests.get(i).inst;
                proposals[i] = acceptRequests.get(i).arg;
            }
        }
        try {
            AcceptAnswer[] answers = remoteAcceptorClient.bulkAccept(instanceIds, proposals);
            for (int i = 0; i < instanceIds.length; ++i) {
                if (acceptRequests.get(i) != null)
                    acceptRequests.get(i).setResponse(answers[i]);
            }
        } catch (IOException e) {
            for (int i = 0; i < instanceIds.length; ++i) {
                if (acceptRequests.get(i) != null)
                    acceptRequests.get(i).setResponse(e);
            }
        }
    }

    @Override
    public synchronized void consensusReached(long inst) {
        if (catchingUp) {
            if (inst >= firstWantedInst && inst <= targetInst) {
                if (!finishedInstances.contains(inst)) {
                    finishedInstances.add(inst);
                    instanceHasArrived(inst);
                }
            }
        }
    }

    synchronized <ARG, RESP> RESP sendRequest(long instanceId, RemoteCallManager.RemoteCallable<RESP> normalCall,
                                              ARG arg, boolean isPrepare) throws IOException {
        int myIndex = (int)(instanceId - firstWantedInst);
        if (catchingUp && instanceId >= firstWantedInst && instanceId <= targetInst && isPrepare == preparing
                && expectedInstances[myIndex]) {

            Request<ARG, RESP> myRequest = new Request<>(instanceId, arg);
            if (isPrepare) {
                //noinspection unchecked
                prepareRequests.set(myIndex, (Request<Proposal.ID, PrepareAnswer>) myRequest);
            } else {
                //noinspection unchecked
                acceptRequests.set(myIndex, (Request<Proposal, AcceptAnswer>) myRequest);
            }
            instanceHasArrived(instanceId);
            if (!myRequest.isFinished()) {
                try {
                    wait(TIMEOUT);
                    // timeout is only in case of bad network state preventing proposer from reaching accept phase
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
            if (myRequest.isFinished()) {
                return myRequest.getResponse();
            } else {
                finish();
                throw new IOException("Bulk catching-up failed");
            }
        } else {
            return normalCall.doRemoteCall();
        }
    }

    private class Request<ARG, RESP> {
        private long inst;
        private ARG arg;
        private RESP response = null;
        private IOException exception = null;

        private Request(long inst, ARG arg) {
            this.inst = inst;
            this.arg = arg;
        }

        private void setResponse(RESP response) {
            this.response = response;
        }

        private void setResponse(IOException exception) {
            this.exception = exception;
        }

        private boolean isFinished() {
            return response != null || exception != null;
        }

        private RESP getResponse() throws IOException {
            if (exception != null)
                throw exception;
            return response;
        }
    }
}
