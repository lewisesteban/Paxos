package com.lewisesteban.paxos.paxosnode.acceptor;

import com.lewisesteban.paxos.paxosnode.proposer.Proposal;

import java.io.Serializable;

public class PrepareAnswer implements Serializable {

    private boolean prepareOK;
    private Proposal alreadyAccepted;
    private boolean snapshotRequestRequired = false;

    PrepareAnswer(boolean prepareOK, Proposal alreadyAccepted) {
        this.prepareOK = prepareOK;
        this.alreadyAccepted = alreadyAccepted;
    }

    PrepareAnswer(boolean prepareOK, Proposal alreadyAccepted, boolean snapshotRequestRequired) {
        this.prepareOK = prepareOK;
        this.alreadyAccepted = alreadyAccepted;
        this.snapshotRequestRequired = snapshotRequestRequired;
    }

    public boolean isPrepareOK() {
        return prepareOK;
    }

    public Proposal getAlreadyAccepted() {
        return alreadyAccepted;
    }

    public boolean isSnapshotRequestRequired() {
        return snapshotRequestRequired;
    }
}
