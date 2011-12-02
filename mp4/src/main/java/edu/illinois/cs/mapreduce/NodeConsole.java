package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import edu.illinois.cs.mapreduce.Job.Phase;
import edu.illinois.cs.mapreduce.Node.NodeServices;
import edu.illinois.cs.mapreduce.Status.State;

public class NodeConsole {

    static enum Command {
        SUBMIT_JOB("submit-job"), STATUS("status"), THROTTLE("throttle"), HELP("help"), STOP_NODE("stop-node");
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

        // parse options
        int paramIdx = 1;
        String host;
        int port;
        if (args.length > 2 && args[1].equals("-n")) {
            String[] target = args[2].split(":");
            host = target[0];
            port = Integer.parseInt(target[1]);
            paramIdx = 3;
        } else {
            host = "localhost";
            port = 60001;
        }

        String[] params = new String[args.length - paramIdx];
        System.arraycopy(args, paramIdx, params, 0, params.length);
        NodeServices services = RPC.newClient(host, port, NodeServices.class);
        switch (cmd) {
            case HELP:
                printUsage();
                break;
            case SUBMIT_JOB:
                if (params.length < 2) {
                    System.out.println("invalid arguments to " + Command.SUBMIT_JOB + " command");
                    printUsage();
                    System.exit(1);
                }
                File jobJarFile = new File(params[0]);
                File inputFile = new File(params[1]);
                JobID id = services.submitJob(jobJarFile, inputFile);
                System.out.println("Job submitted. ID: " + id.toQualifiedString());
                break;
            case STATUS:
                if (params.length == 0) {
                    System.out.println("Node TODO status:");
                } else if (params.length == 1) {
                    JobID jobId = JobID.fromQualifiedString(params[0]);
                    JobStatus job = services.getJobStatus(jobId);
                    System.out.println("Job " + job.getId()
                        + " status: "
                        + (job.getPhase() == Phase.REDUCE && job.getState() == State.SUCCEEDED ? State.SUCCEEDED : job
                            .getPhase() + "-" + job.getState()));
                    ImmutableStatus<JobID> mapStatus = (job.getPhase() == Phase.MAP ? job : job.getMapStatus());
                    System.out.println("  Phase: " + Phase.MAP);
                    System.out.println("    State: " + mapStatus.getState());
                    for (TaskStatus task : job.getMapTaskStatuses()) {
                        System.out.println("    Map Task " + task.getId().getValue());
                        System.out.println("      State: " + task.getState());
                        for (TaskAttemptStatus attempt : task.getAttemptStatuses()) {
                            System.out.println("     Attempt " + attempt.getId().getValue() + ":");
                            System.out.println("       Running on Node:" + attempt.getTargetNodeID());
                            System.out.println("       State: " + attempt.getState());
                            if (attempt.getMessage() != null)
                                System.out.println("       Message: " + attempt.getMessage());
                        }
                    }
                    System.out.println("  Phase: " + Phase.REDUCE);
                    if (job.getPhase() == Phase.REDUCE) {
                        System.out.println("    State: " + job.getState());
                    } else {
                        System.out.println("    State: Not Started");
                    }
                    for (TaskStatus task : job.getReduceTaskStatuses()) {
                        System.out.println("    Reduce Task " + task.getId().getValue());
                        System.out.println("      State: " + task.getState());
                        for (TaskAttemptStatus attempt : task.getAttemptStatuses()) {
                            System.out.println("     Attempt " + attempt.getId().getValue() + ":");
                            System.out.println("       Running on Node:" + attempt.getTargetNodeID());
                            System.out.println("       State: " + attempt.getState());
                            if (attempt.getMessage() != null)
                                System.out.println("       Message: " + attempt.getMessage());
                        }
                    }
                } else if (params.length == 2) {
                    System.out.println("Task TODO status");
                } else if (params.length == 3) {
                    System.out.println("Attempt TODO status");
                }
                break;
            case THROTTLE:
                if (params.length != 1) {
                    System.out.println("invalid arguments to " + Command.THROTTLE + " command");
                    printUsage();
                    System.exit(1);
                }
                int newThrottleValue = 0;
                try {
                    newThrottleValue = Integer.parseInt(params[0]);
                } catch (Exception e) {
                    System.out.println("invalid arguments to " + Command.THROTTLE + " command");
                    printUsage();
                    break;
                }
                if (newThrottleValue < 0 || newThrottleValue > 100) {
                    System.out.println("invalid arguments to " + Command.THROTTLE + " command");
                    printUsage();
                    break;
                }
                services.setThrottle((double)newThrottleValue);
                System.out.println("Throttle value set " + newThrottleValue);
                break;
            case STOP_NODE:
                services.stopNode();
        }
    }

    private static void printUsage() {
        System.out.println("usage: COMMAND [OPTIONS] PARAMETERS");
        System.out.println("OPTIONS");
        System.out.println("      -n {host}:{port}      node to connect the console to");
        System.out.println("COMMANDS");
        System.out.println("      submit-job {job-jar-file} {input-file-path}");
        System.out.println("      status [job-id|job-id task#|job-id task# attempt#]");
        System.out.println("      throttle {integer-value 0-100}");
        System.out.println("      stop-node");
    }
}
