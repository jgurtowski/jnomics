package edu.cshl.schatz.jnomics.mapreduce;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.HashMap;
import java.util.Map;

public abstract class JnomicsReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
        extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    public JnomicsReducer(){}

    public abstract Class getOutputKeyClass();
    public abstract Class getOutputValueClass();
    public JnomicsArgument[] getArgs(){return new JnomicsArgument[0];}
    
    public Class<? extends Partitioner> getPartitionerClass(){return null;}
    public Class<? extends WritableComparator> getGrouperClass(){return null;}
    public Class<? extends OutputFormat> getOutputFormat(){return SequenceFileOutputFormat.class;}

    public Map<String,String> getConfModifiers(){return new HashMap<String, String>();}
}
