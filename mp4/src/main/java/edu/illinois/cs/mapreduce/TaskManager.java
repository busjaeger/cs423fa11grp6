package edu.illinois.cs.mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskManager extends Remote {

    void submitTask(Task task) throws RemoteException;

}