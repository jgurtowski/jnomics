package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * User: james
 */
public class CustomReadKmerFilterMap extends JnomicsMapper<Writable, Text, Text, NullWritable>{

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public Class<? extends InputFormat> getInputFormat() {
        return TextInputFormat.class;
    }

    @Override
    protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split("\t");
        if(arr.length != 2){
            return;
        }
        String []counts = arr[1].split(",");

        int cint;
        int count = 0;
        for(String c : counts){
            cint = Integer.parseInt(c);
            if(cint > 500){
                return;
            }
            if(cint > 20 && cint < 60)
                ++count;
        }
        if(count >= 5){
            context.write(value,NullWritable.get());
        }
    }
}
