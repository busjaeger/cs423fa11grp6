package edu.illinois.cs.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.TimerTask;

public class HardwareMonitorTask extends TimerTask {

	private double cpuUtil;
	private String file;
	private int currentTotal;
	private int currentIdle;
	
	public HardwareMonitorTask() {
		this.cpuUtil = 0.0;
		this.file = "/proc/stat";
		this.currentTotal = 0;
		this.currentIdle = 0;
	}
	
	public double getCpuUtil() {
		return this.cpuUtil;
	}
	
	@Override
	public void run() {
		readCpuUtilization();
		
	}
	
	private void readCpuUtilization() {
		FileReader file;
		String[] values;
		String space = " ";
		int totalTime = 0;
		int idleTime = 0;
		String trimmed;
		try {
			// Get first line from /proc/stat
			file = new FileReader(this.file);
			BufferedReader in = new BufferedReader(file);
			String s = in.readLine();
			in.close();
			
			// Trim leading 'cpu ' and add up all jiffies
			trimmed = s.substring(5);
			values = trimmed.split(space);
			int length = values.length;
			for(int i = 0; i < length; i++) {
				totalTime += Integer.parseInt(values[i]);
			}
			idleTime = Integer.parseInt(values[3]);
			
			// Compute cpu utilization
			//System.out.println("totaltime: " + totalTime + " idletime: " + idleTime);
			double interval = totalTime - currentTotal;
			double idlediff = idleTime - currentIdle;
			double percent = (idlediff / interval) * 100;
			//System.out.println("percent idle: " + percent + " in last: " + interval + " jiffies");
			this.cpuUtil = percent;
			currentIdle = idleTime;
			currentTotal = totalTime;
		} catch (FileNotFoundException e1) {
			System.out.println("HardwareMonitorTask, File Not Found: " + this.file);
			e1.printStackTrace();
		} catch (IOException e) {
			System.out.println("HardwareMonitorTask, IOException.");
			e.printStackTrace();
		}
	}	

}
