package edu.illinois.cs.dlb.api;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import edu.illinois.cs.dlb.Configuration;
import edu.illinois.cs.dlb.JobManager;

public class JobConsole {

    public static void main(String[] args) throws NotBoundException, IOException {
        Configuration config = Configuration.load();
        Registry registry = LocateRegistry.getRegistry(config.getRmiRegistryHost(), config.getRmiRegistryPort());
        Job job = new Job(null, null, null, null, null, null, null);

        while (true) {
            System.out.println("going to sleep");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            System.out.println("waking up");
            try {
                JobManager jobs = (JobManager)registry.lookup("/jobmanager/" + config.getId());
                jobs.submitJob(job);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
