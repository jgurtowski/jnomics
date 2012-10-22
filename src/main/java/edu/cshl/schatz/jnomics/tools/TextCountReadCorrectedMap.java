package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * User: james
 */
public class TextCountReadCorrectedMap extends JnomicsMapper<LongWritable, Text, Text,LongWritable>{

    private Text numReadsCorrected = new Text("Number of Reads Corrected:");
    private LongWritable one = new LongWritable(1);
    
    @Override
    public Class getOutputKeyClass() {
        return Text.class;
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
    public Class<? extends Reducer> getCombinerClass() {
        return TextCountReadCorrectedReduce.class;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String []arr = value.toString().split("\t");
        if(arr.length < 3)
            return;
        if(0 == arr[1].compareTo("SUCCESS") && Integer.parseInt(arr[2]) >0)
            context.write(numReadsCorrected,one);
    }
}
