package sample;

import java.io.IOException;

import edu.illinois.cs.mapreduce.api.Context;
import edu.illinois.cs.mapreduce.api.Reducer;

public class WordCountReducer extends Reducer<String, Long, String, Long> {

    @Override
    public void reduce(String key, Iterable<Long> values, Context<String, Long> context) throws IOException {
        long totalCount = 0;
        for (Long count : values)
            totalCount += count;
        context.write(key, totalCount);
    }

}
