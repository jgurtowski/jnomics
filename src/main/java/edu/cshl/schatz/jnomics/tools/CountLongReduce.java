package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * User: james
 */
public class CountLongReduce extends JnomicsReducer<WritableComparable,LongWritable, WritableComparable, LongWritable> {

    private final LongWritable count_writable = new LongWritable();
    private long count;

    @Override
    public Class getOutputKeyClass() {
        return null; // generic key, will use the output from the mapper
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[0];
    }

    @Override
    protected void reduce(WritableComparable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        count = 0;
        for(LongWritable c: values){
            count += c.get();
        }
        count_writable.set(count);
        context.write(key,count_writable);
    }
}
