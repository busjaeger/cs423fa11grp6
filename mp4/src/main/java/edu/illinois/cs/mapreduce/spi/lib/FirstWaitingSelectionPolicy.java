package edu.illinois.cs.mapreduce.spi.lib;

import static edu.illinois.cs.mr.util.Status.State.CREATED;
import static edu.illinois.cs.mr.util.Status.State.WAITING;
import edu.illinois.cs.mapreduce.spi.SelectionPolicy;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.jm.Job;
import edu.illinois.cs.mr.jm.MapTask;
import edu.illinois.cs.mr.jm.TaskAttempt;
import edu.illinois.cs.mr.util.Status.State;

public class FirstWaitingSelectionPolicy extends SelectionPolicy {

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
                    }
        }
        return candidate;
    }

}
