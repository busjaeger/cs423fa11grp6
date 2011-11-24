package edu.illinois.cs.dlb.api;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import edu.illinois.cs.dlb.Configuration;
import edu.illinois.cs.dlb.Job.JobID;
import edu.illinois.cs.dlb.JobClient;

public class JobConsole {

    static enum Command {
        SUBMIT_JOB("submit-job");
        private final String string;

        private Command(String string) {
            this.string = string;
        }

        public String toString() {
            return string;
        }

        static Command fromString(String s) {
            for (Command cmd : Command.values())
                if (cmd.toString().equals(s))
                    return cmd;
            return null;
        }
    }

    public static void main(String[] args) throws RemoteException, NotBoundException, IOException {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }
        Command cmd = Command.fromString(args[0]);
        if (cmd == null) {
            System.out.println("unknown command: " + args[0]);
            printUsage();
            System.exit(1);
        }

        switch (cmd) {
            case SUBMIT_JOB:
                if (args.length < 3) {
                    System.out.println("invalid arguments to " + Command.SUBMIT_JOB + " command");
                    printUsage();
                    System.exit(1);
                }
                File jobJarFile = new File(args[1]);
                File inputFile = new File(args[2]);
                Configuration config;
                if (args.length > 4 && args[3].equals("-config"))
                    config = Configuration.load(new File(args[4]));
                else
                    config = Configuration.load();
                JobClient jobClient = getStub(config);
                JobID id = jobClient.submitJob(jobJarFile, inputFile);
                System.out.println("Job submitted. ID: " + id);
                break;
        }
    }

    private static JobClient getStub(Configuration config) throws NotBoundException, RemoteException, IOException {
        String host = config.getRmiRegistryHost();
        int port = config.getRmiRegistryPort();
        Registry registry = LocateRegistry.getRegistry(host, port);
        String path = "/jobmanager/" + config.getId();
        return (JobClient)registry.lookup(path);
    }

    private static void printUsage() {
        System.out.println("usage:");
        System.out.println("submit-job {job-jar-file} {input-file-path}");
    }
}
