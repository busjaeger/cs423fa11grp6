package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import edu.illinois.cs.mapreduce.Node.NodeServices;

public class NodeConsole {

    static enum Command {
        SUBMIT_JOB("submit-job"), JOB_STATUS("job-status"), SET_THROTTLE("set-throttle");
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

        // command
        Command cmd = Command.fromString(args[0]);
        if (cmd == null) {
            System.out.println("unknown command: " + args[0]);
            printUsage();
            System.exit(1);
        }

        // target host:port
        String[] target = args[1].split(":");
        String host = target[0];
        int port = Integer.parseInt(target[1]);

        NodeServices services = RPC.newClient(host, port, NodeServices.class);
        switch (cmd) {
            case SUBMIT_JOB:
                if (args.length < 4) {
                    System.out.println("invalid arguments to " + Command.SUBMIT_JOB + " command");
                    printUsage();
                    System.exit(1);
                }
                File jobJarFile = new File(args[2]);
                File inputFile = new File(args[3]);
                JobID id = services.submitJob(jobJarFile, inputFile);
                System.out.println("Job submitted. ID: " + id.toQualifiedString());
                break;
            case JOB_STATUS:
                if (args.length < 2) {
                    System.out.println("invalid arguments to " + Command.JOB_STATUS + " command");
                    printUsage();
                    System.exit(1);
                }
                JobID jobId = JobID.fromQualifiedString(args[2]);
                JobStatus job = services.getJobStatus(jobId);
                System.out.println("Job " + job.getId() + " status:");
                System.out.println("  State:" + job.getState());
                System.out.println("  Phase:" + job.getPhase());
                for (TaskStatus task : job.getMapTaskStatuses()) {
                    System.out.println("  Task " + task.getId());
                    System.out.println("    State:" + task.getState());
                    for (TaskAttemptStatus attempt : task.getAttemptStatuses()) {
                        System.out.println("   Attempt " + attempt.getId() + ":");
                        System.out.println("     Running on Node:" + attempt.getTargetNodeID());
                        System.out.println("     State:" + attempt.getState());
                        if (attempt.getMessage() != null)
                            System.out.println("     Message:" + attempt.getMessage());
                    }
                }
                break;
            case SET_THROTTLE:
                if (args.length < 3) {
                    System.out.println("invalid arguments to " + Command.SET_THROTTLE + " command");
                    printUsage();
                    System.exit(1);
                }
                int newThrottleValue = 0;
                try {
                    newThrottleValue = Integer.parseInt(args[2]);
                } catch (Exception e) {
                    System.out.println("invalid arguments to " + Command.SET_THROTTLE + " command");
                    printUsage();
                    e.printStackTrace();
                    System.exit(1);
                }
                if (newThrottleValue < 0 || newThrottleValue > 100) {
                    System.out.println("invalid arguments to " + Command.SET_THROTTLE + " command");
                    printUsage();
                    System.exit(1);
                }
                services.setThrottle((double)newThrottleValue);
                System.out.println("Throttle value set " + newThrottleValue);
        }
    }

    private static void printUsage() {
        System.out.println("usage:");
        System.out.println("submit-job {job-jar-file} {input-file-path} [-config {config-file}]");
        System.out.println("job-status {job-id} [-config {config-file}]");
        System.out.println("set-throttle {integer-value 0-100}");
    }
}