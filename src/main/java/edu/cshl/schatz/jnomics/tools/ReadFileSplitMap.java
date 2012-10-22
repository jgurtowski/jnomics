package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;


/**
 * Takes reads from a ReadCollectionWritable and just writes them back out
 */
public class ReadFileSplitMap extends JnomicsMapper<ReadCollectionWritable, NullWritable, ReadWritable, NullWritable> {

    @Override
    public Class getOutputKeyClass() {
        return ReadWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    protected void map(ReadCollectionWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        for( ReadWritable read: key.getReads()){
            context.write(read, NullWritable.get());
        }
    }
}

