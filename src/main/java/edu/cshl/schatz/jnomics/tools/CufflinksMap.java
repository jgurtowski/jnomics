/**
 * 
 * @file	-	AlignmentSortMap.java
 * 
 * @purpose	-	Global sorting of files in following format:
 * 				1. SAMRecordWritable - uses RNAME and POS as key
 * 
 * @author 	-	Piyush Kansal
 * 
 */

package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class CufflinksMap extends JnomicsMapper<SAMRecordWritable, NullWritable, SamtoolsMap.SamtoolsKey, SAMRecordWritable>{

    private final SamtoolsMap.SamtoolsKey stkey = new SamtoolsMap.SamtoolsKey();

    @Override
    public Class getOutputKeyClass() {
        return SamtoolsMap.SamtoolsKey.class;
    }

    @Override
    public Class getOutputValueClass() {
        return SAMRecordWritable.class;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        stkey.setBin(0);
    }

    public void map( SAMRecordWritable key, NullWritable value, Context context )
            throws IOException, InterruptedException {
        
        if(0 == key.getMappingQuality().get()) //remove unmapped reads
            return;
        int alignmentStart = key.getAlignmentStart().get();
        stkey.setPosition( alignmentStart );
        stkey.setRef( key.getReferenceName() );
        context.write( stkey, key );
    }
}
