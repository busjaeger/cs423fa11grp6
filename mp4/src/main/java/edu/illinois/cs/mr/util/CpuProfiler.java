package edu.illinois.cs.mr.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;


public class CpuProfiler {

    private static final Pattern p = Pattern.compile(" ");
    private static final String PROC_STAT_FILE = "/proc/stat";

    private int currentTotal;
    private int currentIdle;

    public double readUtilization() throws IOException {
        FileReader file = new FileReader(PROC_STAT_FILE);
        BufferedReader in = new BufferedReader(file);
        String s;
        try {
            s = in.readLine();
        } finally {
            in.close();
        }

        // Get first line from /proc/stat
        // Trim leading 'cpu ' and add up all jiffies
        String trimmed = s.substring(5);
        String[] values = p.split(trimmed);
        int totalTime = 0;
        for (int i = 0; i < values.length; i++)
            totalTime += Integer.parseInt(values[i]);
        int idleTime = Integer.parseInt(values[3]);

        // Compute cpu utilization
        // System.out.println("totaltime: " + totalTime + " idletime: " +
        // idleTime);
        double interval = totalTime - currentTotal;
        double idlediff = idleTime - currentIdle;
        double percent = 100.0 - ((idlediff / interval) * 100);
        currentIdle = idleTime;
        currentTotal = totalTime;
        return percent;
    }

}
