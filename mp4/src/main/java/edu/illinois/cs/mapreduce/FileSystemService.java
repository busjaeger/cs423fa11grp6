package edu.illinois.cs.mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface to either a local or remote file system. This interface provides an
 * input/output stream abstraction to file system users regardless of underlying
 * implementation.
 * 
 * @author benjamin
 */
public interface FileSystemService {

    InputStream read(Path path) throws IOException;

    OutputStream write(Path path) throws IOException;

    void copy(Path dest, File src) throws IOException;

    void copy(Path dest, InputStream is) throws IOException;

    boolean mkdir(Path path) throws IOException;

    boolean delete(Path path) throws IOException;

    boolean exists(Path path) throws IOException;

}
