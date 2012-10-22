package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.writable.BinWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

/**
 * User: james
 */
public class TemplateHistReduce extends JnomicsReducer<BinWritable, IntWritable, BinWritable, IntWritable> {

    private int total;
    private IntWritable total_writable = new IntWritable();
    
    @Override
    public Class getOutputKeyClass() {
        return BinWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return IntWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[0];
    }

    @Override
    protected void reduce(BinWritable bin, Iterable<IntWritable> occurances, Context context) throws IOException, InterruptedException {
        total = 0;

        for(IntWritable occurance: occurances){
            total += occurance.get();
        }
        total_writable.set(total);
        context.write(bin, total_writable);
    }
}
