package edu.illinois.cs.mapreduce;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public final class ReflectionUtil {

    private ReflectionUtil() {
        super();
    }

    public static ClassLoader createClassLoader(File... files) throws MalformedURLException {
        URL[] urls = new URL[files.length];
        for (int i = 0; i < files.length; i++)
            urls[i] = files[i].toURI().toURL();
        return createClassLoader(urls);
    }

    public static ClassLoader createClassLoader(URL... urls) {
        return new URLClassLoader(urls);
    }

    public static <T> T newInstance(String className, NodeConfiguration config) throws ClassNotFoundException,
        IllegalAccessException, InstantiationException, InvocationTargetException, SecurityException,
        NoSuchMethodException {
        ClassLoader cl = ReflectionUtil.class.getClassLoader();
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>)cl.loadClass(className);
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            Constructor<T> cons = clazz.getConstructor(config.getClass());
            return cons.newInstance(config);
        }
    }

    public static <T> T newInstance(String className, ClassLoader classLoader) throws ClassNotFoundException,
        InstantiationException, IllegalAccessException {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>)classLoader.loadClass(className);
        return clazz.newInstance();
    }

    public static <T> T newInstance(String className, File... files) throws MalformedURLException,
        ClassNotFoundException, InstantiationException, IllegalAccessException {
        ClassLoader classLoader = createClassLoader(files);
        return newInstance(className, classLoader);
    }

}
