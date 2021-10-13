package miu.cs522.part1.logstats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author XIII
 */
public class LogWritable implements Writable {

    private IntWritable numOfBytes;
    private IntWritable numOfRequests;

    public LogWritable() {
        numOfBytes = new IntWritable(0);
        numOfRequests = new IntWritable(0);
    }

    public LogWritable(int numOfBytes, int numOfRequests) {
        this.numOfBytes = new IntWritable(numOfBytes);
        this.numOfRequests = new IntWritable(numOfRequests);
    }

    public int getNumOfBytes() {
        return numOfBytes.get();
    }

    public void setNumOfBytes(int numOfBytes) {
        this.numOfBytes.set(numOfBytes);
    }

    public int getNumOfRequests() {
        return numOfRequests.get();
    }

    public void setNumOfRequests(int numOfRequests) {
        this.numOfRequests.set(numOfRequests);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        numOfBytes.write(dataOutput);
        numOfRequests.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        numOfBytes.readFields(dataInput);
        numOfRequests.readFields(dataInput);
    }
}
