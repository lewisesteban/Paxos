package com.lewisesteban.paxos.node.acceptor;

import com.lewisesteban.paxos.node.proposer.Proposal;

import java.io.Serializable;

public class PrepareAnswer implements Serializable {

    private boolean prepareOK;
    private Proposal alreadyAccepted;

    PrepareAnswer(boolean prepareOK, Proposal alreadyAccepted) {
        this.prepareOK = prepareOK;
        this.alreadyAccepted = alreadyAccepted;
    }

    public boolean isPrepareOK() {
        return prepareOK;
    }

    public Proposal getAlreadyAccepted() {
        return alreadyAccepted;
    }
}
