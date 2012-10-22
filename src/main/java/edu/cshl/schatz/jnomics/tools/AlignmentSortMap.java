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

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.AlignmentCollectionWritable;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * 
 * @class	-	AlignmentSortMap
 * @purpose	-	To define a class to implement global sorting
 * @author 	-	Piyush Kansal
 *
 */
public class AlignmentSortMap
        extends JnomicsMapper<AlignmentCollectionWritable, NullWritable, SamtoolsMap.SamtoolsKey, SAMRecordWritable>{

    private final SamtoolsMap.SamtoolsKey stkey = new SamtoolsMap.SamtoolsKey();
    private static final JnomicsArgument binsize_arg = new JnomicsArgument("binsize","bin chromosomes", false);
    private int binsize;

    @Override
    public Class getOutputKeyClass() {
        return SamtoolsMap.SamtoolsKey.class;
    }

    @Override
    public Class getOutputValueClass() {
        return SAMRecordWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{binsize_arg};
    }

    protected void setup( final Context context ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        binsize = conf.getInt(binsize_arg.getName(), 1000000 );
    }
		
    public void map( AlignmentCollectionWritable key, NullWritable value, Context context )
            throws IOException, InterruptedException {

        for(SAMRecordWritable record : key){
            int alignmentStart = record.getAlignmentStart().get();
            stkey.setPosition( alignmentStart );
            stkey.setRef( record.getReferenceName() );
            int bin = alignmentStart / binsize;
            stkey.setBin( bin );
            context.write( stkey, record );
        }
    }
}
