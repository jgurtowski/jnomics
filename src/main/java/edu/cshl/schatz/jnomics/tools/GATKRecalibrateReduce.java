package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.File;
import java.io.IOException;

/**
 * User: james
 **/
public class GATKRecalibrateReduce extends GATKBaseReduce<SAMRecordWritable, NullWritable> {

    private final JnomicsArgument recal_covar_arg = new JnomicsArgument("recal_file","<recal.covar> recalibration file", true);
    private final SAMRecordWritable recordWritable = new SAMRecordWritable(); 
    
    private File recal;
    
    @Override
    public Class getOutputKeyClass() {
        return SAMRecordWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public Class<? extends WritableComparator> getGrouperClass() {
        return SamtoolsReduce.SamtoolsGrouper.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        JnomicsArgument[] args = super.getArgs();
        JnomicsArgument[] newArgs = new JnomicsArgument[args.length+1];
        newArgs[0] = recal_covar_arg;
        System.arraycopy(args,0,newArgs,1,args.length);
        return newArgs;
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        recal = binaries.get(recal_covar_arg.getName());
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> values, Context context)
            throws IOException, InterruptedException {

        super.reduce(key,values,context);

        File recalibratedBam = new File(context.getTaskAttemptID()+".recal.bam");
        String recal_cmd = String.format("java -Xmx4g -jar %s -T TableRecalibration -L %s:%d-%d -R %s -I %s -recalFile %s -o %s",
                gatk_binary,key.getRef(),startRange,endRange,reference_fa,tmpBam,recal,recalibratedBam);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(recal_cmd));

        tmpBam.delete();

        SAMFileReader reader = new SAMFileReader(recalibratedBam,true);
        reader.setValidationStringency(SAMFileReader.ValidationStringency.LENIENT);
        for(SAMRecord record: reader){
            recordWritable.set(record);
            context.write(recordWritable,NullWritable.get());
        }

        recalibratedBam.delete();
    }
}
