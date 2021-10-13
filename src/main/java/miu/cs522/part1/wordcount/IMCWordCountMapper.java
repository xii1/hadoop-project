package miu.cs522.part1.wordcount;

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
public class IMCWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Map<String, Integer> countMap = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] words = value.toString().trim().split("\\s+");

        for (String w : words) {
            if (countMap.containsKey(w)) {
                countMap.put(w, countMap.get(w) + 1);
            } else {
                countMap.put(w, 1);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
    }
}
