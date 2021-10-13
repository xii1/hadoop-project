package miu.cs522.part3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author XIII
 */
public class RelativeFrequenciesStripeApproachMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    private Map<String, MapWritable> countMap = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] items = value.toString().trim().toUpperCase().split("\\s+");

        for (int i = 0; i < items.length; ++i) {
            if (!countMap.containsKey(items[i])) {
                countMap.put(items[i], new MapWritable());
            }

            final MapWritable mw = countMap.get(items[i]);
            int j = i + 1;
            while (j < items.length && !items[j].equals(items[i])) {
                final Text text = new Text(items[j]);
                if (mw.containsKey(text)) {
                    final int count = ((IntWritable) mw.get(text)).get() + 1;
                    ((IntWritable) mw.get(text)).set(count);
                } else {
                    mw.put(text, new IntWritable(1));
                }
                ++j;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, MapWritable> entry : countMap.entrySet()) {
            context.write(new Text(entry.getKey()), entry.getValue());
        }
    }
}
