package edu.illinois.cs.mapreduce.spi.lib;

import static edu.illinois.cs.mr.util.Status.State.CREATED;
import static edu.illinois.cs.mr.util.Status.State.WAITING;

import java.util.LinkedList;

import edu.illinois.cs.mapreduce.spi.SelectionPolicy;
import edu.illinois.cs.mr.NodeID;
import edu.illinois.cs.mr.jm.AttemptStatus;
import edu.illinois.cs.mr.jm.JobStatus;
import edu.illinois.cs.mr.jm.Phase;
import edu.illinois.cs.mr.jm.TaskStatus;
import edu.illinois.cs.mr.util.Status.State;

public class WaitingTaskSelectionPolicy implements SelectionPolicy {

    @Override
    public AttemptStatus selectAttempt(NodeID source, NodeID target, Iterable<JobStatus> jobs) {
        for (JobStatus job : reverse(jobs)) {
            // we currently do not swap reducers
            taskLoop : for (TaskStatus task : reverse(job.getTaskStatuses(Phase.MAP))) {
                AttemptStatus candidate = null;
                for (AttemptStatus attempt : reverse(task.getAttemptStatuses())) {
                    NodeID runningOn = attempt.getTargetNodeID();
                    // if this task has a running attempt on that node, don't select it
                    if (runningOn.equals(target) && !attempt.isDone())
                        continue taskLoop;

                    // we found an attempt that's running on the source
                    State state = attempt.getState();
                    if (candidate == null && runningOn.equals(source) && state == CREATED || state == WAITING)
                            candidate = attempt;
                }
                if (candidate != null)
                    return candidate;
            }
        }
        return null;
    }

    private static <T> Iterable<T> reverse(Iterable<T> i) {
        LinkedList<T> list = new LinkedList<T>();
        for (T t : i)
            list.addFirst(t);
        return list;
    }
}
