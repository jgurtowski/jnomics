package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * User: james
 */
public class ReadKmerAnalysisMap extends JnomicsMapper<Writable, Text, IntWritable, LongWritable>{

    private IntWritable countWritable = new IntWritable();
    private LongWritable one = new LongWritable(1);

    private JnomicsArgument lowerBound_arg = new JnomicsArgument("lower_bound","Lower Range Bound",true);
    private JnomicsArgument upperBound_arg = new JnomicsArgument("upper_bound","Upper Range Bound",true);

    private int lower, upper;
    
    @Override
    public Class getOutputKeyClass() {
        return IntWritable.class;
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
        return CountLongReduce.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{lowerBound_arg,upperBound_arg};
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        lower = conf.getInt(lowerBound_arg.getName(),-1);
        upper = conf.getInt(upperBound_arg.getName(),-1);
        if(lower < 0 || upper < 0)
            throw new IOException("Unacceptable lower/upper bound "+lower +","+upper);
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
            if(cint >=lower && cint <=upper){
                count += 1;
            }
        }
        if(count > 0){
            countWritable.set(count);
            context.write(countWritable,one);
        }
    }
}
