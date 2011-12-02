package edu.illinois.cs.mapreduce;

import static edu.illinois.cs.mapreduce.Status.State.CREATED;
import static edu.illinois.cs.mapreduce.Status.State.RUNNING;
import static edu.illinois.cs.mapreduce.Status.State.WAITING;
import edu.illinois.cs.mapreduce.Status.State;

public interface SelectionPolicy {

    public static class DefaultSelectionPolicy implements SelectionPolicy {
        @Override
        public TaskAttempt selectAttempt(NodeID nodeID, Iterable<Job> jobs) {
            TaskAttempt candidate = null;
            for (Job job : jobs) {
                // we currently do not swap reducers
                for (MapTask task : job.getMapTasks())
                    for (TaskAttempt attempt : task.getAttempts())
                        if (attempt.getTargetNodeID().equals(nodeID)) {
                            State state = attempt.getState();
                            if (state == CREATED || state == WAITING)
                                return attempt;
                            else if (state == RUNNING)
                                candidate = attempt;
                        }
            }
            return candidate;
        }
    }

    TaskAttempt selectAttempt(NodeID nodeID, Iterable<Job> jobs);

}
