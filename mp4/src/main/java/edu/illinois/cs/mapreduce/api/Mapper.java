package edu.illinois.cs.mapreduce.api;

import java.io.IOException;

/**
 * Implemented by user to transform key/value pairs produced by a
 * {@link RecordReader} into intermediate key/value pairs stored in the
 * {@link Context}
 * 
 * @author benjamin
 * @param <KI> type of the input key to this Mapper
 * @param <VI> type of the input value to this Mapper
 * @param <KO> type of the output keys produced by this Mapper
 * @param <VO> type of the output values produced by this Mapper
 */
public abstract class Mapper<KI, VI, KO, VO> {

    /**
     * @param key input key to the map function
     * @param value input value to the map function
     * @param context context for reporting status and storing output keys and
     *            values
     * @throws IOException any IOException encountered during processing
     */
    public abstract void map(KI key, VI value, Context<KO, VO> context) throws IOException;

}
