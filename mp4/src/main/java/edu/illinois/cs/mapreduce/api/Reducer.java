package edu.illinois.cs.mapreduce.api;

import java.io.IOException;

/**
 * Reduces a set of values for a given key to one or more output key/value pairs
 * 
 * @author benjamin
 * @param <KI> type of input key
 * @param <VI> type of input value
 * @param <KO> type of output key
 * @param <VO> type of output value
 */
public abstract class Reducer<KI, VI, KO, VO> {

    /**
     * reduces the given key and set of values to one or more output key/value
     * pairs
     * 
     * @param key key for the values
     * @param values set of values to reduce
     * @param context context for storing results and reporting status
     * @throws IOException if an error occurred reducing the current key and
     *             value set
     */
    public abstract void reduce(KI key, Iterable<VI> values, Context<KO, VO> context) throws IOException;

}
