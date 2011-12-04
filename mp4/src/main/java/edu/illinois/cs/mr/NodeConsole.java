package edu.illinois.cs.mr;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import edu.illinois.cs.mr.jm.JobID;
import edu.illinois.cs.mr.jm.JobStatus;
import edu.illinois.cs.mr.jm.Phase;
import edu.illinois.cs.mr.jm.AttemptStatus;
import edu.illinois.cs.mr.jm.TaskStatus;
import edu.illinois.cs.mr.util.ImmutableStatus;
import edu.illinois.cs.mr.util.RPC;

/**
 * Console used to interact with a node. The console issues commands specified
 * by users against the remote node.
 * 
 * @author benjamin
 */
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
        NodeService services = RPC.newClient(host, port, NodeService.class);
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

                    System.out.println("\nJob " + job.getId() + " status: " + job.getState() + "\n");
                    printTimes(job, "");

                    for (Phase phase : Phase.values())
                        printPhase(job, phase);
                } else if (params.length == 2) {
                    System.out.println("Task status TODO");
                } else if (params.length == 3) {
                    System.out.println("Attempt status TODO");
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
                services.stop();
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

    private static void printPhase(JobStatus job, Phase phase) {
        System.out.println("  Phase: " + phase);
        ImmutableStatus<JobID> status = job.getPhaseStatus(phase);
        if (status == null) {
            System.out.println("    State: Not Started");
        } else {
            System.out.println("    State: " + status.getState());
            printTimes(status, "    ");
            printTasks(job.getTaskStatuses(phase), phase);
        }
    }

    private static void printTasks(Iterable<TaskStatus> statuses, Phase phase) {
        for (TaskStatus task : statuses) {
            System.out.println("    " + phase + " Task #" + task.getId().getValue());
            System.out.println("      State: " + task.getState());
            printTimes(task, "      ");
            for (AttemptStatus attempt : task.getAttemptStatuses()) {
                System.out.println("      Attempt " + attempt.getId().getValue());
                System.out.println("        State: " + attempt.getState());
                System.out.println("        On Node:" + attempt.getTargetNodeID());
                if (attempt.getMessage() != null)
                    System.out.println("        Message: " + attempt.getMessage());
                printTimes(attempt, "        ");
            }
        }
    }

    private static void printTimes(ImmutableStatus<?> status, String prefix) {
        if (status.isDone()) {
            long created = status.getCreatedTime();
            long waiting = status.getBeginWaitingTime();
            long running = status.getBeginRunningTime();
            long done = status.getDoneTime();
            long total = done - created;
            System.out.println(prefix + "Total time: " + total);
            // we may not know each event, due to our heart beat frequency
            // neither known
            if (waiting == -1 && running == -1) {
                System.out.println(prefix + "Waiting time: n/a");
                System.out.println(prefix + "Running time: n/a");
            }
            // only running time known
            else if (waiting == -1) {
                System.out.println(prefix + "Waiting time: n/a");
                System.out.println(prefix + "Running time: " + (done - running));
            }
            // only queue time known
            else if (running == -1) {
                System.out.println(prefix + "Waiting time: n/a");
                System.out.println(prefix + "Running time: n/a");
            }
            // both known
            else {
                System.out.println(prefix + "Waiting time: " + (running - waiting));
                System.out.println(prefix + "Running time: " + (done - running));
            }
        }
    }

}
