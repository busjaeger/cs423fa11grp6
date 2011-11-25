package edu.illinois.cs.dfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

public interface FileSystem {

    URL toURL(Path path) throws IOException;

    InputStream read(Path path) throws IOException;

    OutputStream write(Path path) throws IOException;

    void copy(Path dest, File src) throws IOException;

    void copy(Path dest, InputStream is) throws IOException;

}