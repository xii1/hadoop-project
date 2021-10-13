package miu.cs522.part1.logstats;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author XIII
 */
public class IMCAvgBytesSentReducer extends Reducer<Text, LogWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<LogWritable> values, Context context) throws IOException, InterruptedException {
        int totalBytes = 0;
        int totalRequests = 0;

        for (LogWritable v : values) {
            totalBytes += v.getNumOfBytes();
            totalRequests += v.getNumOfRequests();
        }

        context.write(key, new DoubleWritable((double) totalBytes / totalRequests));
    }
}
