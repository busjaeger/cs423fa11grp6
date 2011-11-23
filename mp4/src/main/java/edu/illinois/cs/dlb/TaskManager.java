package edu.illinois.cs.dlb;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteOutputStream;

public interface TaskManager extends Remote {

    RemoteOutputStream open(Path path) throws RemoteException, IOException;

    void write(Path to, RemoteInputStream from) throws RemoteException, IOException;

}