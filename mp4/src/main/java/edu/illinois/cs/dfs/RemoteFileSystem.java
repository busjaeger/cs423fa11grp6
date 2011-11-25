package edu.illinois.cs.dfs;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteOutputStream;


public interface RemoteFileSystem extends Remote {

    RemoteInputStream read(Path path) throws RemoteException, IOException;

    RemoteOutputStream write(Path path) throws RemoteException, IOException;

    void copy(Path dest, RemoteInputStream src) throws RemoteException, IOException;

}