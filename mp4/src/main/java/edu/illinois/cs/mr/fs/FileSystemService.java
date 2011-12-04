package edu.illinois.cs.mr.fs;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface to either a local or remote file system. This interface provides an
 * input/output stream abstraction to file system users regardless of underlying
 * implementation.
 * 
 * @author benjamin
 */
public interface FileSystemService {

    /**
     * Returns an InputStream to read data from the file located at the given
     * path. The client of the method must close the InputStream when done
     * reading from it.
     * 
     * @param path Path to the file to read
     * @return InputStream InputStream connected to the file
     * @throws IOException If no file exists at the given path or an error
     *             occurs performing the remove invocation
     */
    InputStream read(Path path) throws IOException;

    /**
     * Writes the data from the input stream to a file at the given path. If no
     * file exists at the given path, it will be created, otherwise it will be
     * overwritten. Any parent directories specified in the path that currently
     * do no exist are created. Data from the input stream is written until it
     * is exhausted. Users of this method are responsible for closing the
     * InputStream.
     * 
     * @param path Path to the file to be written
     * @param is InputStream holding data to be written
     * @throws IOException If an error occurs when reading data from the
     *             InputStream, writing to the file, or performing a remote
     *             invocation.
     */
    void write(Path path, InputStream is) throws IOException;

    /**
     * Deletes the file or directory at the given path.
     * 
     * @param path Path to the file or folder to delete
     * @return true if the file or folder at the given path was fully deleted,
     *         false otherwise
     * @throws IOException if an error occurred invoking this method remotely.
     */
    boolean delete(Path path) throws IOException;

    /**
     * Checks if a file or folder exists at the given path.
     * 
     * @param path Path to the file or folder to check
     * @return true if a file or folder exists at the given path, false
     *         otherwise
     * @throws IOException if an error occurred invoking this method remotely
     */
    boolean exists(Path path) throws IOException;

}
