package edu.illinois.cs.mapreduce.api;

import java.io.IOException;

public abstract class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    public abstract void reduce(KEYIN key, Iterable<VALUEIN> values, Context<KEYOUT, VALUEOUT> context)
        throws IOException;

}
