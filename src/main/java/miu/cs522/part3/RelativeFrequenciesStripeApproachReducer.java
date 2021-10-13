package miu.cs522.part3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author XIII
 */
public class RelativeFrequenciesStripeApproachReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        final MapWritable temp = new MapWritable();

        int total = 0;
        for (MapWritable v : values) {
            for (Map.Entry<Writable, Writable> entry : v.entrySet()) {
                total += ((IntWritable) entry.getValue()).get();

                if (temp.containsKey(entry.getKey())) {
                    final int sum = ((IntWritable) temp.get(entry.getKey())).get() + ((IntWritable) entry.getValue()).get();
                    ((IntWritable) temp.get(entry.getKey())).set(sum);
                } else {
                    temp.put(entry.getKey(), entry.getValue());
                }
            }
        }

        final MapWritable result = new MapWritable(){
            @Override
            public String toString() {
                return String.format("{%s}",
                        this.entrySet()
                                .stream()
                                .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(", ")));
            }
        };

        for (Map.Entry<Writable, Writable> entry : temp.entrySet()) {
            result.put(entry.getKey(), new DoubleWritable((double) ((IntWritable) entry.getValue()).get() / total));
        }

        context.write(key, result);
    }
}
