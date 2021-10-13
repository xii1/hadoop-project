package miu.cs522.part2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author XIII
 */
public class RelativeFrequenciesPairApproachReducer extends Reducer<PairItem, IntWritable, PairItem, DoubleWritable> {

    private Integer totalCountByFirstItem;

    @Override
    protected void reduce(PairItem key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if ("*".equals(key.getSecond().toString())) {
            totalCountByFirstItem = 0;
            for (IntWritable v : values) {
                totalCountByFirstItem += v.get();
            }
        } else {
            int totalCount = 0;
            for (IntWritable v : values) {
                totalCount += v.get();
            }

            context.write(key, new DoubleWritable((double) totalCount / totalCountByFirstItem));
        }
    }
}
