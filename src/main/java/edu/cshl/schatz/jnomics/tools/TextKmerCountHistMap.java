package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.FixedKmerWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;


/**
 * Takes Output from KCounter and creates histogram of the kmer counts
 */
public class TextKmerCountHistMap extends JnomicsMapper<LongWritable, Text, LongWritable, LongWritable> {

    private final LongWritable one = new LongWritable(1);
    private final LongWritable count = new LongWritable();
    
    @Override
    public Class getOutputKeyClass() {
        return LongWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public Class<? extends InputFormat> getInputFormat() {
        return TextInputFormat.class;
    }

    @Override
    public Class<? extends Reducer> getCombinerClass(){
        return TextKmerCountHistReduce.class;
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String []arr= value.toString().split("\t");
        if(arr.length != 2){
            return;
        }
        count.set(Long.parseLong(arr[1]));
        context.write(count,one);
    }
}

