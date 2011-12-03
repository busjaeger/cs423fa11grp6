package edu.illinois.cs.mr.util;

import java.io.File;
import java.io.IOException;
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

    public static <T, P> T newInstance(String className, Class<? super P> paramClass, P param) throws IOException {
        try {
            ClassLoader cl = ReflectionUtil.class.getClassLoader();
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>)cl.loadClass(className);
            try {
                return clazz.newInstance();
            } catch (InstantiationException e) {
                Constructor<T> cons = clazz.getConstructor(paramClass);
                return cons.newInstance(param);
            }
        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (NoSuchMethodException e) {
            throw new IOException(e);
        } catch (InvocationTargetException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public static <T> T newInstance(String className, ClassLoader classLoader) throws IOException {
        try {
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>)classLoader.loadClass(className);
            return clazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }

    }

    public static <T> T newInstance(String className, File... files) throws IOException {
        ClassLoader classLoader = createClassLoader(files);
        return newInstance(className, classLoader);
    }

}
