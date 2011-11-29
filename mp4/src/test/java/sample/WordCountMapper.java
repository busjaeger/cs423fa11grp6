package sample;

import java.io.IOException;
import java.util.StringTokenizer;

import edu.illinois.cs.mapreduce.api.Context;
import edu.illinois.cs.mapreduce.api.Mapper;

public class WordCountMapper extends Mapper<Long, String, String, Long> {

    @Override
    public void map(Long key, String value, Context<String, Long> context)
        throws IOException {
        StringTokenizer tokenizer = new StringTokenizer(value);
        while (tokenizer.hasMoreTokens())
            context.write(tokenizer.nextToken(), 1l);
    }

}
