package miu.cs522.part3;

import miu.cs522.mapreduce.MapReduceJob;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author XIII
 */
public class RelativeFrequenciesStripeApproachJob
        extends MapReduceJob<RelativeFrequenciesStripeApproachMapper, RelativeFrequenciesStripeApproachReducer> {

    public RelativeFrequenciesStripeApproachJob() throws IOException {
        super("Relative Frequencies Job using Stripe Approach",
                RelativeFrequenciesStripeApproachMapper.class, RelativeFrequenciesStripeApproachReducer.class,
                Text.class, MapWritable.class);
    }
}
