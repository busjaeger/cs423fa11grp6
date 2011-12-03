package edu.illinois.cs.mapreduce.spi.lib;

import static edu.illinois.cs.mr.util.Status.State.CREATED;
import static edu.illinois.cs.mr.util.Status.State.WAITING;
import edu.illinois.cs.mapreduce.spi.SelectionPolicy;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.jm.AttemptStatus;
import edu.illinois.cs.mr.jm.JobStatus;
import edu.illinois.cs.mr.jm.Phase;
import edu.illinois.cs.mr.jm.TaskStatus;
import edu.illinois.cs.mr.util.Status.State;

public class FirstWaitingSelectionPolicy implements SelectionPolicy {

    @Override
    public AttemptStatus selectAttempt(NodeID source, Iterable<JobStatus> jobs) {
        AttemptStatus candidate = null;
        for (JobStatus job : jobs) {
            // we currently do not swap reducers
            for (TaskStatus task : job.getTaskStatuses(Phase.MAP))
                for (AttemptStatus attempt : task.getAttemptStatuses())
                    if (attempt.getTargetNodeID().equals(source)) {
                        State state = attempt.getState();
                        if (state == CREATED || state == WAITING)
                            return attempt;
                    }
        }
        return candidate;
    }

}
