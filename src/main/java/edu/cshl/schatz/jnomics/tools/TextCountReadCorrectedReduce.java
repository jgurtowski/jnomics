package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * User: james
 */
public class TextCountReadCorrectedReduce extends JnomicsReducer<Text,LongWritable, Text, LongWritable> {

    private long count;
    private LongWritable longWritable = new LongWritable();

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return TextOutputFormat.class;
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        count =0;
        for(LongWritable lw: values){
            count += lw.get();
        }
        longWritable.set(count);
        context.write(key,longWritable);
    }
}
