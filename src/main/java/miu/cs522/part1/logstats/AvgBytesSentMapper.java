package miu.cs522.part1.logstats;

import miu.cs522.part1.parser.ApacheLogParser;
import miu.cs522.part1.parser.ApacheLogRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author XIII
 */
public class AvgBytesSentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final ApacheLogRecord logRecord = ApacheLogParser.getInstance().parse(value.toString());

        if (logRecord != null) {
            context.write(new Text(logRecord.getIpAddress()), new IntWritable(logRecord.getBytesSent()));
        }
    }
}
