package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * User: james
 */
public class ReadKmerFilterMap extends JnomicsMapper<Writable, Text, Text, NullWritable>{

    private JnomicsArgument kupperbound_arg = new JnomicsArgument("kupper_bound", "Upper bound of global kmer depth", true);
    private JnomicsArgument klowerbound_arg = new JnomicsArgument("klower_bound", "Lower bound of global kmer depth", true);
    private JnomicsArgument perreadlower_arg = new JnomicsArgument("klower_perread", "lower bound number of times reads from kupper and klower bound can be found in read",true);
    private JnomicsArgument perreadupper_arg = new JnomicsArgument("kupper_perread", "lower bound number of times reads from kupper and klower bound can be found in read",true);

    private int kupper,klower,perread_upper,perread_lower;

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
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{kupperbound_arg, klowerbound_arg,perreadlower_arg,perreadupper_arg};
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        kupper = conf.getInt(kupperbound_arg.getName(),-1);
        klower = conf.getInt(klowerbound_arg.getName(),-1);
        perread_upper = conf.getInt(perreadupper_arg.getName(),-1);
        perread_lower = conf.getInt(perreadlower_arg.getName(),-1);
        if(kupper<0 || klower<0 || perread_lower< 0 || perread_upper < 0)
            throw new IOException("Invalid bounds "+kupper+","+klower+","+perread_lower+","+perread_upper);
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
            if(cint >= klower && cint <= kupper){
                count += 1;
            }
        }
        if(count >= perread_lower && count <= perread_upper){
            context.write(value,NullWritable.get());
        }
    }
}
