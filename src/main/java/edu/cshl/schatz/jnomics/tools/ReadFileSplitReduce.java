package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Author: James
 */

public class ReadFileSplitReduce extends JnomicsReducer<ReadWritable, NullWritable, Text, NullWritable> {

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return TextOutputFormat.class;
    }

    @Override
    protected void reduce(ReadWritable read, final Iterable<NullWritable> values, final Context context)
            throws IOException, InterruptedException {
        
        context.write(read.getSequence(), NullWritable.get());
    }
}
