package miu.cs522.part2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author XIII
 */
public class RelativeFrequenciesPairApproachMapper extends Mapper<LongWritable, Text, PairItem, IntWritable> {

    private final Map<PairItem, Integer> countMap = new HashMap<>();

    private void countPairItem(PairItem item) {
        if (countMap.containsKey(item)) {
            countMap.put(item, countMap.get(item) + 1);
        } else {
            countMap.put(item, 1);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] items = value.toString().trim().toUpperCase().split("\\s+");

        for (int i = 0; i < items.length; ++i) {
            int j = i + 1;
            while (j < items.length && !items[j].equals(items[i])) {
                countPairItem(new PairItem(items[i], items[j]));
                countPairItem(new PairItem(items[i], "*"));
                ++j;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<PairItem, Integer> entry : countMap.entrySet()) {
            context.write(entry.getKey(), new IntWritable(entry.getValue()));
        }
    }
}
