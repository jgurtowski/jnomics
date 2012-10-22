package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import net.sf.samtools.SAMSequenceRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * User: james
 * Base class reducer for many GATK Operations
 */

public abstract class GATKBaseReduce<VALIN,VALOUT>
        extends JnomicsReducer<SamtoolsMap.SamtoolsKey,SAMRecordWritable,VALIN,VALOUT> {

    private static Logger logger = LoggerFactory.getLogger(GATKBaseReduce.class);
    
    private final JnomicsArgument gatk_jar_arg = new JnomicsArgument("gatk_jar","GATK jar file",true);
    private final JnomicsArgument samtools_bin_arg = new JnomicsArgument("samtools_binary","Samtools binary file",true);
    private final JnomicsArgument reference_fa_arg = new JnomicsArgument("reference_fa", "Reference genome", true);

    protected Map<String,File> binaries = new HashMap<String, File>();

    protected File samtools_binary, reference_fa, gatk_binary, tmpBam;

    protected long binsize, startRange, endRange;

    @Override
    public Class<? extends Partitioner> getPartitionerClass() {
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public Class<? extends WritableComparator> getGrouperClass() {
        return SamtoolsReduce.SamtoolsGrouper.class;
    }


    @Override
    public Map<String,String> getConfModifiers(){
        return new HashMap<String, String>(){
            {
                put("mapred.reduce.tasks.speculative.execution","false");
            }
        };
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{gatk_jar_arg, samtools_bin_arg,reference_fa_arg};
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        File f = null;
        for(JnomicsArgument binary_arg : getArgs()){
            f =  new File(conf.get(binary_arg.getName(),""));
            if(!f.exists())
                throw new IOException("Missing : " + binary_arg.getName() + ":have:" + f);
            binaries.put(binary_arg.getName(),f);
        }

        samtools_binary = binaries.get(samtools_bin_arg.getName());
        reference_fa = binaries.get(reference_fa_arg.getName());
        gatk_binary = binaries.get(gatk_jar_arg.getName());

        binsize =context.getConfiguration().getLong(SamtoolsMap.genome_binsize_arg.getName(),
                SamtoolsMap.DEFAULT_GENOME_BINSIZE);

        /**Symlink reference_fa to local dir, GATK has locking issues on the .dict file**/
        File local_reference_fa = new File(reference_fa.getName());
        Process p = Runtime.getRuntime().exec("ln -s " + reference_fa + " " + local_reference_fa);
        p.waitFor();
        p = Runtime.getRuntime().exec("ln -s "+reference_fa+".fai"+" "+local_reference_fa);
        p.waitFor();
        reference_fa = local_reference_fa;
    }

    /**
     * Base Reducer Writes the current sengments Alignments to a local Bam file.
     * This is a common process in all of the GATK steps
     */

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> values, Context context)
            throws IOException, InterruptedException {

        tmpBam = new File(context.getTaskAttemptID()+".tmp.bam");
        /**Write bam to temp file**/
        String samtools_convert_cmd = String.format("%s view -Sb -o %s -", samtools_binary, tmpBam);
        logger.info(samtools_convert_cmd);
        Process samtools_convert = Runtime.getRuntime().exec(samtools_convert_cmd);

        Thread errConn = new Thread(new ThreadedStreamConnector(samtools_convert.getErrorStream(),System.err));
        Thread outConn = new Thread(new ThreadedStreamConnector(samtools_convert.getInputStream(),System.out));
        outConn.start();errConn.start();

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(samtools_convert.getOutputStream()));
        int ref_len=-1;
        long count = 0;
        for(SAMRecordWritable record: values){
            if(0 == count){
                writer.write(record.getTextHeader()+"\n");
                SAMSequenceRecord headerSequence = record.getSAMRecord().getHeader().getSequence(key.getRef().toString());
                if(null != headerSequence)
                    ref_len = headerSequence.getSequenceLength();
                if(ref_len < 0)
                    throw new IOException("Could not get Reference Length from header");
            }
            writer.write(record.toString()+"\n");
            if(0 == ++count % 1000 ){
                context.progress();
            }
        }

        startRange = key.getBin().get() * binsize + 1;
        endRange = startRange + binsize - 1;
        endRange = endRange > ref_len ? ref_len : endRange;
        logger.info("Working on Region:" + key.getRef() + ":" + startRange + "-" + endRange);

        writer.close();
        samtools_convert.waitFor();
        outConn.join();errConn.join();

        /**Index Bam**/
        String samtools_idx_cmd = String.format("%s index %s", samtools_binary, tmpBam);
        ProcessUtil.exceptionOnError(ProcessUtil.execAndReconnect(samtools_idx_cmd));
    }
}
