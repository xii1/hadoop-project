package miu.cs522.part1.wordcount;

import miu.cs522.mapreduce.MapReduceJob;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WordCountJob extends MapReduceJob<WordCountMapper, WordCountReducer, Text, IntWritable> {

    public WordCountJob() throws IOException {
        super("Word Count Job", WordCountMapper.class, WordCountReducer.class, Text.class, IntWritable.class);
    }
}
