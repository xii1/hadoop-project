package miu.cs522.part1.logstats;

import miu.cs522.part1.parser.ApacheLogParser;
import miu.cs522.part1.parser.ApacheLogRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author XIII
 */
public class IMCAvgBytesSentMapper extends Mapper<LongWritable, Text, Text, LogWritable> {

    private final Map<String, LogWritable> countMap = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final ApacheLogRecord logRecord = ApacheLogParser.getInstance().parse(value.toString());

        if (logRecord != null) {
            final String ipAddress = logRecord.getIpAddress();
            final int bytesSent = logRecord.getBytesSent();

            if (countMap.containsKey(ipAddress)) {
                final LogWritable logWritable = countMap.get(ipAddress);
                logWritable.setNumOfBytes(logWritable.getNumOfBytes() + bytesSent);
                logWritable.setNumOfRequests(logWritable.getNumOfRequests() + 1);
            } else {
                countMap.put(ipAddress, new LogWritable(bytesSent, 1));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, LogWritable> entry : countMap.entrySet()) {
            context.write(new Text(entry.getKey()), entry.getValue());
        }
    }
}
