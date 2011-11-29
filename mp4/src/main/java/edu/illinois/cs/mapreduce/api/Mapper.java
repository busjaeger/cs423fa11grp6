package edu.illinois.cs.mapreduce.api;

import java.io.IOException;

/**
 * @param <K>
 * @param <V>
 */
public abstract class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     */
    public abstract void map(KEYIN key, VALUEIN value, Context<KEYOUT, VALUEOUT> context) throws IOException;

}
