import miu.cs522.mapreduce.MapReduceJob;
import miu.cs522.part1.logstats.AvgBytesSentJob;
import miu.cs522.part1.logstats.IMCAvgBytesSentJob;
import miu.cs522.part1.wordcount.IMCWordCountJob;
import miu.cs522.part1.wordcount.WordCountJob;
import miu.cs522.part2.RelativeFrequenciesPairApproachJob;
import miu.cs522.part3.RelativeFrequenciesStripeApproachJob;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author XIII
 */
public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(String.format("Usage: %s %s <input> <output>",
                    Main.class.getName(),
                    "<p1a|p1b|p1c|p1d|p2|p3>"));
            return;
        }

        MapReduceJob job = null;
        switch (args[0]) {
            case "p1a": job = new WordCountJob();
            break;
            case "p1b": job = new IMCWordCountJob();
            break;
            case "p1c": job = new AvgBytesSentJob();
            break;
            case "p1d": job = new IMCAvgBytesSentJob();
            break;
            case "p2": job = new RelativeFrequenciesPairApproachJob();
            break;
            case "p3": job = new RelativeFrequenciesStripeApproachJob();
            break;
        }

        final int statusCode = ToolRunner.run(job, args);;
        System.exit(statusCode);
    }
}
