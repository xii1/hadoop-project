package miu.cs522.part2;

import miu.cs522.mapreduce.MapReduceJob;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

/**
 * @author XIII
 */
public class RelativeFrequenciesPairApproachJob
        extends MapReduceJob<RelativeFrequenciesPairApproachMapper, RelativeFrequenciesPairApproachReducer> {

    public RelativeFrequenciesPairApproachJob() throws IOException {
        super("Relative Frequencies Job using Pair Approach",
                RelativeFrequenciesPairApproachMapper.class, RelativeFrequenciesPairApproachReducer.class,
                PairItem.class, IntWritable.class);

        job.setPartitionerClass(PairItemPartitioner.class);
    }
}
