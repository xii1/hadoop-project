package miu.cs522.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @author XIII
 */
public class MapReduceJob<M extends Mapper, R extends Reducer> extends Configured implements Tool {

    protected Job job;

    public <K, V> MapReduceJob(String jobName, Class<M> mapperClass, Class<R> reducerClass,
                        Class<K> mapOutputKeyClass, Class<V> mapOutputValueClass) throws IOException {
        initJob(jobName, mapperClass, reducerClass);

        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
    }

    public MapReduceJob(String jobName, Class<M> mapperClass, Class<R> reducerClass) throws IOException {
        initJob(jobName, mapperClass, reducerClass);
    }

    private void initJob(String jobName, Class<M> mapperClass, Class<R> reducerClass) throws IOException {
        final Configuration conf = new Configuration();
        job = Job.getInstance(conf, jobName);
        job.setJarByClass(this.getClass());
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
    }

    @Override
    public int run(String[] args) throws Exception {
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        final Path output = new Path(args[2]);
        output.getFileSystem(getConf()).delete(output, true);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
