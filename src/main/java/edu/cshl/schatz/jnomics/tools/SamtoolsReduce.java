package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.Command;
import edu.cshl.schatz.jnomics.util.OutputStreamHandler;
import edu.cshl.schatz.jnomics.util.ProcessUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: James
 * Launches Samtools
 */

public class SamtoolsReduce extends JnomicsReducer<SamtoolsMap.SamtoolsKey, SAMRecordWritable, Text, NullWritable> {

    private int reduceIt, binsize;

    private final JnomicsArgument samtools_bin_arg = new JnomicsArgument("samtools_binary","Samtools Binary",true);
    private final JnomicsArgument bcftools_bin_arg = new JnomicsArgument("bcftools_binary","bcftools Binary", true);
    private final JnomicsArgument samtools_opts_arg = new JnomicsArgument("samtools_opts","Samtools mpileup options", false);
    private final JnomicsArgument bcftools_opts_arg = new JnomicsArgument("bcftools_opts","bcftools view options", false);
    private final JnomicsArgument reference_file_arg = new JnomicsArgument("reference_fa",
            "Reference Fasta (must be indexed)", true);

    private FileSystem hdfs;

    private String samtools_bin, bcftools_bin, reference_file, samtools_opts, bcftools_opts;
    
    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public Class<? extends WritableComparator> getGrouperClass(){
        return SamtoolsReduce.SamtoolsGrouper.class;
    }

    @Override
    public Class<? extends Partitioner> getPartitionerClass(){
        return SamtoolsReduce.SamtoolsPartitioner.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{samtools_bin_arg,bcftools_bin_arg,reference_file_arg,samtools_opts_arg,bcftools_opts_arg};
    }

    public static class SamtoolsGrouper extends WritableComparator {

        public SamtoolsGrouper(){
            super(SamtoolsMap.SamtoolsKey.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            SamtoolsMap.SamtoolsKey first = (SamtoolsMap.SamtoolsKey)a;

            SamtoolsMap.SamtoolsKey second = (SamtoolsMap.SamtoolsKey)b;
            int diff;
            if((diff=first.getRef().compareTo(second.getRef())) == 0)
                diff =first.getBin().compareTo(second.getBin());
            return diff;
        }
    }

    @Override
    public Map<String,String> getConfModifiers(){
        return new HashMap<String, String>(){
            {
                put("mapred.reduce.tasks.speculative.execution","false");
            }
        };
    }

    public static class SamtoolsPartitioner extends Partitioner<SamtoolsMap.SamtoolsKey,SAMRecordWritable> {

        private HashPartitioner<String, NullWritable> partitioner = new HashPartitioner<String, NullWritable>();

        @Override
        public int getPartition(SamtoolsMap.SamtoolsKey samtoolsKey, SAMRecordWritable samRecordWritable, int i) {
            String ref = samtoolsKey.getRef().toString();
            String bin = String.valueOf(samtoolsKey.getBin().get());
            return partitioner.getPartition(ref+"-"+bin,NullWritable.get(), i);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String binsize_str = conf.get(SamtoolsMap.genome_binsize_arg.getName());
        binsize = binsize_str == null ? SamtoolsMap.DEFAULT_GENOME_BINSIZE : Integer.parseInt(binsize_str);
        hdfs = FileSystem.get(conf);

        samtools_bin = conf.get(samtools_bin_arg.getName());
        bcftools_bin = conf.get(bcftools_bin_arg.getName());
        reference_file = conf.get(reference_file_arg.getName());
        samtools_opts = conf.get(samtools_opts_arg.getName(),"");
        bcftools_opts = conf.get(bcftools_opts_arg.getName(),"");

    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, final Iterable<SAMRecordWritable> values, final Context context)
            throws IOException, InterruptedException {

        System.out.println("Begin Samtools Reduce");

        System.out.println("Writing temp bam files");
        /**Setup temp bam file**/
        String taskAttemptId = context.getTaskAttemptID().toString();
        File tmpBam = new File(taskAttemptId+"_"+(reduceIt++)+".bam");

        /**launch sam-to-bam conversion and write entries to process**/
        String samtoolsS2B = String.format("%s view -Sb - -o %s", samtools_bin, tmpBam.getAbsolutePath());

        ProcessUtil.runCommandEZ(new Command(samtoolsS2B, new OutputStreamHandler() {
            @Override
            public void handle(OutputStream out) {
                boolean first =  true;
                PrintWriter writer = new PrintWriter(out);

                for(SAMRecordWritable record: values){
                    if(first){
                        writer.println(record.getTextHeader());
                        first = false;
                    }
                    context.progress();
                    writer.println(record);
                }
                writer.close();
            }
        }));
        
        /** Index the temp bam**/
        String samtoolsIdxCmd = String.format("%s index %s", samtools_bin, tmpBam.getAbsolutePath());
        ProcessUtil.runCommandEZ(new Command(samtoolsIdxCmd));

        System.out.println("Running mpileup/bcftools snp operation");
        /** Run mpileup on indexed bam and pipe output to bcftools **/
        int binstart = key.getBin().get() * binsize;
        System.out.println(String.format("Region: %s:%d-%d",key.getRef().toString(),binstart,binstart+binsize-1));
        String samtoolsCmd = String.format("%s mpileup %s -r %s:%d-%d -uf %s %s",
                samtools_bin, samtools_opts, key.getRef().toString(),
                binstart, binstart+binsize-1, reference_file, tmpBam.getAbsolutePath());
        String bcftoolsCmd = String.format("%s view %s -vcg -", bcftools_bin, bcftools_opts);
        final Process samtoolsProcess = Runtime.getRuntime().exec(samtoolsCmd);
        final Process bcftoolsProcess = Runtime.getRuntime().exec(bcftoolsCmd);

        /**connect mpileup output to bcftools input **/
        Thread sambcfLink = new Thread(
                new ThreadedStreamConnector(samtoolsProcess.getInputStream(), bcftoolsProcess.getOutputStream()){
                    @Override
                    public void progress(){
                        context.progress();
                    }
                }
        );

        sambcfLink.start();

        /**reconnect stderr for debugging **/
        Thread samtoolsProcessErr = new Thread(new ThreadedStreamConnector(samtoolsProcess.getErrorStream(),System.err));
        Thread bcftoolsProcessErr = new Thread(new ThreadedStreamConnector(bcftoolsProcess.getErrorStream(),System.err));
        samtoolsProcessErr.start();
        bcftoolsProcessErr.start();
        String out = context.getConfiguration().get("mapred.output.dir");
        Path outFile = new Path(out,key.getRef()+"-"+key.getBin()+".vcf");
        OutputStream outStream = hdfs.create(outFile);
        Thread bcfReaderThread = new Thread(new ThreadedStreamConnector(bcftoolsProcess.getInputStream(),outStream));
        bcfReaderThread.start();
        bcfReaderThread.join();
        outStream.close();

        samtoolsProcess.waitFor();
        bcftoolsProcess.waitFor();
        sambcfLink.join();
        samtoolsProcessErr.join();
        bcftoolsProcessErr.join();
        File tmpBamIdx = new File(tmpBam.getAbsolutePath()+".bai");
        tmpBam.delete();
        tmpBamIdx.delete();
        System.out.println("Complete, cleaning up");
    }
}
