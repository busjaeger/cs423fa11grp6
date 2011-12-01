package edu.illinois.cs.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;

import edu.illinois.cs.mapreduce.RPCServer.Invocation;

class RPCClient implements InvocationHandler {

    @SuppressWarnings("unchecked")
    public static <T> T newProxy(String host, int port, Class<?>... interf) throws IOException {
        InvocationHandler handler = new RPCClient(host, port);
        return (T)Proxy.newProxyInstance(RPCClient.class.getClassLoader(), interf, handler);
    }

    private final String host;
    private final int port;

    public RPCClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Socket socket = new Socket(host, port);
        ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
        try {
            Invocation invocation = new Invocation(method.getName(), method.getParameterTypes());
            os.writeObject(invocation);
            int last = args.length - 1;
            for (int i = 0; i < last; i++)
                os.writeObject(args[i]);
            if (InputStream.class.isAssignableFrom(invocation.parameterClasses[last]))
                FileUtil.transfer((InputStream)args[last], os);
            else
                os.writeObject(args[last]);
            
            
            
            
        } finally {
            os.close();
        }

        return null;
    }
}
