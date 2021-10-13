package miu.cs522.part1.logstats;

import miu.cs522.mapreduce.MapReduceJob;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author XIII
 */
public class AvgBytesSentJob extends MapReduceJob<AvgBytesSentMapper, AvgBytesSentReducer, Text, IntWritable> {

    public AvgBytesSentJob() throws IOException {
        super("Average Computation Job for Bytes Sent", AvgBytesSentMapper.class, AvgBytesSentReducer.class,
                Text.class, IntWritable.class);
    }
}
