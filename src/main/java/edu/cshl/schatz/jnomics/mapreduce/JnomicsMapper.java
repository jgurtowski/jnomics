package edu.cshl.schatz.jnomics.mapreduce;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.HashMap;
import java.util.Map;


public abstract class JnomicsMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
        extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    public JnomicsMapper(){}

    public abstract Class getOutputKeyClass();
    public abstract Class getOutputValueClass();

    public Map<String,String> getConfModifiers(){return new HashMap<String, String>();}

    public JnomicsArgument[] getArgs(){return new JnomicsArgument[0];}
    
    public Class<? extends Reducer> getCombinerClass(){return null;}
    
    public Class<? extends InputFormat> getInputFormat(){return SequenceFileInputFormat.class;}
    
    public Class<? extends OutputFormat> getOutputFormat(){ return SequenceFileOutputFormat.class;}

}                

