package com.lewisesteban.paxos.paxosnode;

import java.io.Serializable;

public interface StateMachine {

    Serializable execute(Serializable data);
}
