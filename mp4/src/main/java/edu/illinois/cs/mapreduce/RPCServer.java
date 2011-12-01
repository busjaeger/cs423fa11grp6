package edu.illinois.cs.mapreduce;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

class RPCServer implements Runnable {
    private final ExecutorService executorService;
    private final ServerSocket serverSocket;
    private final Class<?> serverInterface;
    private final Object server;
    private volatile boolean stopped;

    public RPCServer(ExecutorService executorService, ServerSocket serverSocket, Class<?> serverInterface, Object server) {
        this.executorService = executorService;
        this.serverSocket = serverSocket;
        this.serverInterface = serverInterface;
        this.server = server;
    }

    public void stop() {
        stopped = true;
    }

    public void run() {
        while (!stopped) {
            Socket socket;
            try {
                socket = serverSocket.accept();
                ServerConnection connection = new ServerConnection(socket);
                executorService.execute(connection);
            } catch (IOException e) {
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
            Method method;
            try {
                ObjectInputStream is = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                try {
                    Invocation invocation = (Invocation)is.readObject();
                    Object[] parameters = new Object[invocation.parameterClasses.length];
                    int last = parameters.length - 1;
                    for (int i = 0; i < last; i++)
                        parameters[i] = is.readObject();
                    if (InputStream.class.isAssignableFrom(invocation.parameterClasses[last]))
                        parameters[last] = is;
                    else
                        parameters[last] = is.readObject();
                    method = serverInterface.getMethod(invocation.methodName, invocation.parameterClasses);
                    result = method.invoke(server, parameters);
                } catch (Throwable t) {
                    error = t;
                }

                OutputStream os = new BufferedOutputStream(socket.getOutputStream());
                ObjectOutputStream oos = new ObjectOutputStream(os);
                if (result != null) {
                    if (result instanceof InputStream) {
                        InputStream resultIs = (InputStream)result;
                        try {
                            FileUtil.transfer(resultIs, oos);
                        } finally {
                            resultIs.close();
                        }
                    } else {
                        oos.writeObject(result);
                    }
                } else if (error != null) {
                    oos.writeObject(error);
                }
                os.close();
                is.close();
                socket.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
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
}
