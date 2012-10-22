package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.FixedKmerWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Takes Output from KCounter and creates histogram of the kmer counts
 */
public class KCounterHistMap extends JnomicsMapper<FixedKmerWritable, LongWritable, LongWritable, LongWritable> {

    private final LongWritable one = new LongWritable(1);
    
    @Override
    public Class getOutputKeyClass() {
        return LongWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public Class<? extends Reducer> getCombinerClass(){
        return KCounterHistReduce.class;
    }
    
    @Override
    protected void map(FixedKmerWritable key, LongWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(value,one);
    }
}

