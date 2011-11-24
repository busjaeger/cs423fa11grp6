package sample;

import java.io.IOException;

import edu.illinois.cs.dlb.api.Mapper;

public class SampleMapper extends Mapper<Long, String> {

    @Override
    public void map(Long key, String value, edu.illinois.cs.dlb.api.Mapper.Context<Long, String> context)
        throws IOException {
        throw new UnsupportedOperationException("not yet implemented");
    }

}
