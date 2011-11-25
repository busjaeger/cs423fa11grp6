package edu.illinois.cs.dfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.RemoteException;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;

public class RemoteFileSystemAdapter implements RemoteFileSystem {

    private final FileSystem fileSystem;

    public RemoteFileSystemAdapter(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public RemoteInputStream read(Path path) throws RemoteException, IOException {
        InputStream is = fileSystem.read(path);
        return new SimpleRemoteInputStream(is).export();
    }

    @Override
    public RemoteOutputStream write(Path path) throws RemoteException, IOException {
        OutputStream os = fileSystem.write(path);
        return new SimpleRemoteOutputStream(os).export();
    }

    @Override
    public void copy(Path dest, RemoteInputStream src) throws IOException {
        InputStream is = RemoteInputStreamClient.wrap(src);
        fileSystem.copy(dest, is);
    }

}