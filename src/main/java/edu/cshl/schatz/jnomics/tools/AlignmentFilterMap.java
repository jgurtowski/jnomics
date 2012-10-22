package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsCounter;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.AlignmentCollectionWritable;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;

/**
 * User: james
 */
public class AlignmentFilterMap extends
        JnomicsMapper<AlignmentCollectionWritable, NullWritable, AlignmentCollectionWritable, NullWritable> {

    private static final JnomicsArgument flags_arg = new JnomicsArgument("flags","Flags to filter", true);
    private static final JnomicsArgument invert_arg = new JnomicsArgument("invert","Invert filter [false]",false);

    private int flags;
    private boolean invert;

    Counter in_count,total_count;
    
    @Override
    public Class getOutputKeyClass() {
        return AlignmentCollectionWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{flags_arg,invert_arg};
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        flags = Integer.parseInt(conf.get(flags_arg.getName()),16);
        invert = conf.getBoolean(invert_arg.getName(),false);
        in_count = context.getCounter(JnomicsCounter.Filter.IN);
        total_count = context.getCounter(JnomicsCounter.Filter.TOTAL);
    }

    /** If one read in the a pair matches the filter, both are included **/
    @Override
    protected void map(AlignmentCollectionWritable collection, NullWritable value, Context context)
            throws IOException, InterruptedException {

        boolean in = false;
        for(SAMRecordWritable alignment : collection){
            int readFlags = alignment.getFlags().get();
            if((!invert && flags == (readFlags & flags))
                    || (invert && flags != (readFlags & flags))){
                in = true;
                break;
            }
        }
        if(in)
            context.write(collection,value);
        total_count.increment(collection.size());
    }
}
