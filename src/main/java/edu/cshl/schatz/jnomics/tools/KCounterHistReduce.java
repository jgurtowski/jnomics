package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class KCounterHistReduce extends JnomicsReducer <LongWritable, LongWritable, LongWritable, LongWritable> {

    private long totalCount;

    private final LongWritable totalCountWritable = new LongWritable();
    
    @Override
    public Class getOutputKeyClass() {
        return LongWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        totalCount = 0;
        for(LongWritable count: values){
            totalCount += count.get();
        }
        totalCountWritable.set(totalCount);
        context.write(key,totalCountWritable);
    }
}
