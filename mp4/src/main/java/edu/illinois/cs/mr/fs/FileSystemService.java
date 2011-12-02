package edu.illinois.cs.mr.fs;

import java.io.IOException;
import java.io.InputStream;

import edu.illinois.cs.mr.Node.NodeService;

/**
 * Interface to either a local or remote file system. This interface provides an
 * input/output stream abstraction to file system users regardless of underlying
 * implementation.
 * 
 * @author benjamin
 */
public interface FileSystemService extends NodeService {

    InputStream read(Path path) throws IOException;

    void write(Path dest, InputStream is) throws IOException;

    boolean mkdir(Path path) throws IOException;

    boolean delete(Path path) throws IOException;

    boolean exists(Path path) throws IOException;

}
