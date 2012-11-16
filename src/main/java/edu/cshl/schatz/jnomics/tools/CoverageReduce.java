package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import edu.cshl.schatz.jnomics.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: James
 * Launches: Calculates depth of coverage
 * using Samtools
 */

public class CoverageReduce extends JnomicsReducer<SamtoolsMap.SamtoolsKey, SAMRecordWritable, Text, NullWritable> {

    private int reduceIt, binsize;

    private final JnomicsArgument samtools_bin_arg = new JnomicsArgument("samtools_binary","Samtools Binary",true);

    private FileSystem hdfs;

    private String samtools_bin;
    
    private Path outputDir;
    
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
        return new JnomicsArgument[]{samtools_bin_arg};
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
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String binsize_str = conf.get(SamtoolsMap.genome_binsize_arg.getName());
        outputDir = new Path(conf.get("mapred.output.dir"));
        
        binsize = binsize_str == null ? SamtoolsMap.DEFAULT_GENOME_BINSIZE : Integer.parseInt(binsize_str);
        hdfs = FileSystem.get(conf);
        samtools_bin = conf.get(samtools_bin_arg.getName());
        if( ! new File(samtools_bin).exists()){
            throw new IOException("Cannot Find samtools_bin: " + samtools_bin);
        }else{
            System.out.println("Found Samtools Binary: " + samtools_bin);
        }
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, final Iterable<SAMRecordWritable> values, final Context context)
            throws IOException, InterruptedException {

        System.out.println("Begin Samtools Reduce");

        /**Setup temp bam file**/
        String taskAttemptId = context.getTaskAttemptID().toString();
        File tmpBam = new File(taskAttemptId+"_"+(reduceIt++)+".bam");
        
        System.out.println("Writing temp bam file " + tmpBam);
        
        String samtoolsView = String.format("%s view -Sb - -o %s", samtools_bin, tmpBam.getAbsolutePath());
        
        ProcessUtil.runCommandEZ(new Command(samtoolsView,new OutputStreamHandler(){
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

        String samtoolsIdxCmd = String.format("%s index %s", samtools_bin, tmpBam.getAbsolutePath());
        System.out.println(samtoolsIdxCmd);
        ProcessUtil.runCommandEZ(new Command(samtoolsIdxCmd));


        int binstart = key.getBin().get() * binsize;
        System.out.println(String.format("Region: %s:%d-%d",key.getRef().toString(),binstart,binstart+binsize-1));

        System.out.println("Running Coverage operation");
        String samtoolsCmd = String.format("%s depth -r %s:%d-%d %s",
                samtools_bin, key.getRef().toString(),
                binstart, binstart+binsize-1, tmpBam.getAbsolutePath());
        System.out.println(samtoolsCmd);

        Path hdfsOut = new Path(outputDir,key.getRef()+"-"+key.getBin());
        FSDataOutputStream outFileStream = hdfs.create(hdfsOut);

        //run samtools and write data back to hdfs
        ProcessUtil.runCommandEZ(
                new Command(samtoolsCmd,
                        new DefaultInputStreamHandler(outFileStream),
                        new DefaultInputStreamHandler(System.err)
                )
        );
        
        outFileStream.close();
        File tmpBamIdx = new File(tmpBam.getAbsolutePath()+".bai");
        tmpBam.delete();
        tmpBamIdx.delete();
        System.out.println("Complete, cleaning up");
    }
}
