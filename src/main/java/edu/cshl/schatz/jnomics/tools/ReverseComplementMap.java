package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.*;
import edu.cshl.schatz.jnomics.util.SequenceOpts;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

/**
 * User: james
 */
public class ReverseComplementMap extends JnomicsMapper<Writable, NullWritable, ReadCollectionWritable, NullWritable>{

    private final ReadCollectionWritable collectionWritable = new ReadCollectionWritable();

    @Override
    public Class getOutputKeyClass() {
        return ReadCollectionWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    protected void map(Writable key, NullWritable value, Context context) throws IOException, InterruptedException {
        if(key instanceof ReadCollectionWritable){
            collectionWritable.clear();
            for(ReadWritable read :((ReadCollectionWritable) key)){
                read.setSequence(SequenceOpts.reverseComplement(read.getSequence().toString()));
                read.setQuality(new StringBuffer(read.getQuality().toString()).reverse().toString());
                collectionWritable.addRead(read);
            }
        }
        
        if(key instanceof AlignmentCollectionWritable){
            collectionWritable.clear();
            for(SAMRecordWritable record: (AlignmentCollectionWritable) key){
                ReadWritable read = new ReadWritable();
                read.setAll(record.getReadName().toString(),
                        SequenceOpts.reverseComplement(record.getReadString().toString()),
                        "",
                        new StringBuffer(record.getQualityString().toString()).reverse().toString());
                collectionWritable.addRead(read);
            }
        }

        context.write(collectionWritable,value);
    }
}
