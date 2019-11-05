package com.lewisesteban.paxos.paxosnode.membership;

import com.lewisesteban.paxos.paxosnode.MembershipGetter;

class Bully {
    private MembershipGetter membership;

    Bully(MembershipGetter membership) {
        this.membership = membership;
    }

    void startElection() {

    }
}
