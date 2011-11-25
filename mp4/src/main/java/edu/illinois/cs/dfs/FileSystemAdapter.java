package edu.illinois.cs.dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamClient;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;

public class FileSystemAdapter implements FileSystem {

    private final RemoteFileSystem remoteFS;

    public FileSystemAdapter(RemoteFileSystem remoteFS) {
        this.remoteFS = remoteFS;
    }

    @Override
    public URL toURL(Path path) throws IOException {
        throw new UnsupportedOperationException("toURL currently not supported for remote file systems");
    }

    @Override
    public InputStream read(Path path) throws IOException {
        RemoteInputStream ris = remoteFS.read(path);
        return RemoteInputStreamClient.wrap(ris);
    }

    @Override
    public OutputStream write(Path path) throws IOException {
        RemoteOutputStream ros = remoteFS.write(path);
        return RemoteOutputStreamClient.wrap(ros);
    }

    @Override
    public void copy(Path dest, File src) throws IOException {
        FileInputStream is = new FileInputStream(src);
        try {
            copy(dest, is);
        } finally {
            is.close();
        }
    }

    @Override
    public void copy(Path dest, InputStream is) throws IOException {
        RemoteInputStream ris = new SimpleRemoteInputStream(is);
        remoteFS.copy(dest, ris);
    }

}