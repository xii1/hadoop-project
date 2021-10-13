package miu.cs522.part2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author XIII
 */
public class PairItemPartitioner extends Partitioner<PairItem, IntWritable> {

    @Override
    public int getPartition(PairItem pairItem, IntWritable intWritable, int i) {
        return (i > 0) ? Math.abs(pairItem.getFirst().toString().hashCode()) % i : 0;
    }
}
