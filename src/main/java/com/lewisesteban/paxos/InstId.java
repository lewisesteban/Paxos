package com.lewisesteban.paxos;

public class InstId {

    private int seriesId;
    private int commandId;

    public InstId(int seriesId, int commandId) {
        this.seriesId = seriesId;
        this.commandId = commandId;
    }

    public int getSeriesId() {
        return seriesId;
    }

    public int getCommandId() {
        return commandId;
    }
}
