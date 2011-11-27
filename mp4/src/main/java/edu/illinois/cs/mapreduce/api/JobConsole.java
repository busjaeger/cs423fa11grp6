package edu.illinois.cs.mapreduce.api;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import edu.illinois.cs.mapreduce.JobID;
import edu.illinois.cs.mapreduce.JobManagerService;
import edu.illinois.cs.mapreduce.Node;

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

        String cfgPath = args.length > 2 && args[args.length - 2].equals("-config") ? args[args.length - 1] : null;
        Node.init(cfgPath);
        JobManagerService jobManager = Node.lookup(Node.config.nodeId, JobManagerService.class);

        switch (cmd) {
            case SUBMIT_JOB:
                if (args.length < 3) {
                    System.out.println("invalid arguments to " + Command.SUBMIT_JOB + " command");
                    printUsage();
                    System.exit(1);
                }
                File jobJarFile = new File(args[1]);
                File inputFile = new File(args[2]);
                JobID id = jobManager.submitJob(jobJarFile, inputFile);
                System.out.println("Job submitted. ID: " + id);
                break;
        }
    }

    private static void printUsage() {
        System.out.println("usage:");
        System.out.println("submit-job {job-jar-file} {input-file-path}");
    }
}
