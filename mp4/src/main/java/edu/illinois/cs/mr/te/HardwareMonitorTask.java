package edu.illinois.cs.mr.te;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.TimerTask;


public class HardwareMonitorTask extends TimerTask {

    private static final String PROC_STAT_FILE = "/proc/stat";

    private final TaskExecutor executor;

    private int currentTotal;
    private int currentIdle;

    public HardwareMonitorTask(TaskExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void run() {
        try {
            // Get first line from /proc/stat
            FileReader file = new FileReader(PROC_STAT_FILE);
            BufferedReader in = new BufferedReader(file);
            String s;
            try {
                s = in.readLine();
            } finally {
                in.close();
            }

            // Trim leading 'cpu ' and add up all jiffies
            String trimmed = s.substring(5);
            String[] values = trimmed.split(" ");
            int length = values.length;
            int totalTime = 0;
            for (int i = 0; i < length; i++)
                totalTime += Integer.parseInt(values[i]);
            int idleTime = Integer.parseInt(values[3]);

            // Compute cpu utilization
            // System.out.println("totaltime: " + totalTime + " idletime: " +
            // idleTime);
            double interval = totalTime - currentTotal;
            double idlediff = idleTime - currentIdle;
            double percent = 100.0 - ((idlediff / interval) * 100);
            // System.out.println("percent not idle: " + percent + " in last: "
            // + interval + " jiffies");

            this.executor.setCpuUtilization(percent);
            currentIdle = idleTime;
            currentTotal = totalTime;
        } catch (FileNotFoundException e1) {
            System.out.println("HardwareMonitorTask, File Not Found: " + PROC_STAT_FILE);
            e1.printStackTrace();
        } catch (IOException e) {
            System.out.println("HardwareMonitorTask, IOException.");
            e.printStackTrace();
        }
    }

}
