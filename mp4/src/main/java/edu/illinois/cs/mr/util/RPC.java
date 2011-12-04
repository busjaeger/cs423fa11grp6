package edu.illinois.cs.mr.util;

import static java.lang.Math.min;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

public class RPC {

    public static RPCServer newServer(ExecutorService executorService, int port, Class<?> serverInterface, Object server) {
        return new RPCServer(executorService, port, serverInterface, server);
    }

    @SuppressWarnings("unchecked")
    public static <T> T newClient(String host, int port, Class<T> interf) {
        InvocationHandler handler = new RPCClient(host, port);
        return (T)Proxy.newProxyInstance(RPCClient.class.getClassLoader(), new Class<?>[] {interf}, handler);
    }

    private static final int CHUNK_SIZE = 0x100000; // 1M
    private static final int BUF_SIZE = 0x1000; // 4K

    public static class RPCServer implements Runnable {

        private final ExecutorService executorService;
        private final Class<?> serverInterface;
        private final Object server;
        private final int port;

        private ServerSocket serverSocket;
        private volatile boolean stopped;

        private RPCServer(ExecutorService executorService, int port, Class<?> serverInterface, Object server) {
            this.executorService = executorService;
            this.port = port;
            this.serverInterface = serverInterface;
            this.server = server;
        }

        public void start() throws IOException {
            this.serverSocket = new ServerSocket(port);
            executorService.submit(this);
        }

        public void stop() {
            stopped = true;
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void run() {
            while (!stopped) {
                Socket socket;
                try {
                    socket = serverSocket.accept();
                    ServerConnection connection = new ServerConnection(socket);
                    executorService.execute(connection);
                } catch (IOException e) {
                    if (stopped)
                        break;
                    e.printStackTrace();
                }
            }
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        class ServerConnection implements Runnable {
            private final Socket socket;

            public ServerConnection(Socket socket) {
                this.socket = socket;
            }

            @Override
            public void run() {
                Throwable error = null;
                Object result = null;
                Method method = null;
                try {
                    ObjectInputStream is = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                    try {
                        Invocation invocation = (Invocation)is.readObject();
                        method = serverInterface.getMethod(invocation.methodName, invocation.parameterClasses);
                        Object[] parameters = new Object[invocation.parameterClasses.length];
                        if (invocation.parameterClasses.length > 0) {
                            int last = parameters.length - 1;
                            for (int i = 0; i < last; i++)
                                parameters[i] = is.readObject();
                            if (InputStream.class.isAssignableFrom(invocation.parameterClasses[last]))
                                parameters[last] = new ChunkedInputStream<ObjectInputStream>(is);
                            else
                                parameters[last] = is.readObject();
                        }
                        result = method.invoke(server, parameters);
                    } catch (Throwable t) {
                        error = t;
                    }

                    ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                    os.writeBoolean(error == null);
                    if (error != null) {
                        os.writeObject(error);
                        os.flush();
                    } else if (!Void.TYPE.equals(method.getReturnType())) {
                        if (InputStream.class.isAssignableFrom(method.getReturnType())) {
                            InputStream resultIs = (InputStream)result;
                            try {
                                chunkTransfer(resultIs, os);
                            } finally {
                                resultIs.close();
                            }
                        } else {
                            os.writeObject(result);
                        }
                    }
                    os.flush();
                } catch (Throwable t) {
                    t.printStackTrace();
                } finally {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static class RPCClient implements InvocationHandler {

        private final String host;
        private final int port;

        public RPCClient(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object result = null;
            final Socket socket = new Socket(host, port);
            boolean close = true;
            try {
                final ObjectOutputStream os =
                    new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                Invocation invocation = new Invocation(method.getName(), method.getParameterTypes());
                os.writeObject(invocation);
                if (args != null && args.length > 0) {
                    int last = args.length - 1;
                    for (int i = 0; i < last; i++)
                        os.writeObject(args[i]);
                    if (InputStream.class.isAssignableFrom(invocation.parameterClasses[last])) {
                        chunkTransfer((InputStream)args[last], os);
                    } else {
                        os.writeObject(args[last]);
                    }
                }
                os.flush();

                final ObjectInputStream is = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                if (!is.readBoolean()) {
                    throw (Throwable)is.readObject();
                }
                if (!Void.TYPE.equals(method.getReturnType())) {
                    if (InputStream.class.isAssignableFrom(method.getReturnType())) {
                        result = new ChunkedInputStream<ObjectInputStream>(is) {
                            @Override
                            public void close() throws IOException {
                                socket.close();
                            }
                        };
                        close = false;
                    } else {
                        result = is.readObject();
                    }
                }
            } finally {
                if (close)
                    socket.close();
            }
            return result;
        }
    }

    public static class Invocation implements Serializable {
        private static final long serialVersionUID = 2421491011501698883L;

        final String methodName;
        final Class<?>[] parameterClasses;

        public Invocation(String methodName, Class<?>[] parameterClasses) {
            this.methodName = methodName;
            this.parameterClasses = parameterClasses;
        }
    }

    private static class ChunkedInputStream<T extends InputStream & DataInput> extends InputStream {

        private final T is;
        private int remaining = 0;

        ChunkedInputStream(T is) throws IOException {
            this.is = is;
        }

        @Override
        public int read() throws IOException {
            if (eof())
                return -1;
            remaining -= 1;
            return is.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            if (eof())
                return -1;
            int read = is.read(b, 0, min(b.length, remaining));
            remaining -= read;
            return read;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (eof())
                return -1;
            int read = is.read(b, off, min(len, remaining));
            remaining -= read;
            return read;
        }

        private boolean eof() throws IOException {
            if (remaining == 0)
                remaining = is.readInt();
            return remaining == 0;
        }
    }

    private static void chunkTransfer(InputStream is, DataOutput os) throws IOException {
        byte[] chunk = new byte[CHUNK_SIZE];
        int read = CHUNK_SIZE;
        while (read == CHUNK_SIZE) {
            read = readChunk(is, chunk);
            if (read == 0)
                break;
            os.writeInt(read);
            int remaining = read, off = 0;
            while (remaining > 0) {
                int len = min(remaining, BUF_SIZE);
                os.write(chunk, off, len);
                off += len;
                remaining -= len;
            }
        }
        os.writeInt(0);
    }

    private static int readChunk(InputStream is, byte[] chunk) throws IOException {
        int read, remaining = CHUNK_SIZE, off = 0;
        while (remaining > 0) {
            int len = min(remaining, BUF_SIZE);
            read = is.read(chunk, off, len);
            if (read == -1)
                return off;
            off += read;
            remaining -= read;
        }
        return CHUNK_SIZE;
    }
}
