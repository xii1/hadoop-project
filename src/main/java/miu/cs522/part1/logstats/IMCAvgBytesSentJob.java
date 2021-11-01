package miu.cs522.part1.logstats;

import miu.cs522.mapreduce.MapReduceJob;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author XIII
 */
public class IMCAvgBytesSentJob extends MapReduceJob<IMCAvgBytesSentMapper, IMCAvgBytesSentReducer> {

    public IMCAvgBytesSentJob() throws IOException {
        super("Average Computation Job for Bytes Sent with In-Mapper Combining",
                IMCAvgBytesSentMapper.class, IMCAvgBytesSentReducer.class, Text.class, LogWritable.class);
    }
}
