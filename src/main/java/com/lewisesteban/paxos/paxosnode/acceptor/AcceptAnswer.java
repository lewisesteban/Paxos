package com.lewisesteban.paxos.paxosnode.acceptor;

import java.io.Serializable;

public class AcceptAnswer implements Serializable {
    public static final int REFUSED = 0;
    public static final int ACCEPTED = 1;
    public static final int SNAPSHOT_REQUEST_REQUIRED = 2;

    private int answer;

    public AcceptAnswer(int answer) {
        this.answer = answer;
    }

    public int getAnswer() {
        return answer;
    }
}
