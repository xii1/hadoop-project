package miu.cs522.part1.logstats;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author XIII
 */
public class AvgBytesSentReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalBytes = 0;
        int totalRequests = 0;

        for (IntWritable v : values) {
            totalBytes += v.get();
            ++totalRequests;
        }

        context.write(key, new DoubleWritable((double) totalBytes / totalRequests));
    }
}
