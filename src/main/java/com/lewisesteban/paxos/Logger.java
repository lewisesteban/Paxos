package com.lewisesteban.paxos;

public class Logger {

    private static boolean ON = true;

    public static void println(String str) {
        if (ON) {
            System.out.println(str);
        }
    }

    public static boolean isOn() {
        return ON;
    }
}
