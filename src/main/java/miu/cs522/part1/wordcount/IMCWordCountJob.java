package miu.cs522.part1.wordcount;

import miu.cs522.mapreduce.MapReduceJob;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class IMCWordCountJob extends MapReduceJob<IMCWordCountMapper, WordCountReducer> {

    public IMCWordCountJob() throws IOException {
        super("Word Count Job with In-Mapper Combining",
                IMCWordCountMapper.class, WordCountReducer.class, Text.class, IntWritable.class);
    }
}
