package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.AlignmentCollectionWritable;
import edu.cshl.schatz.jnomics.ob.FastqStringProvider;
import edu.cshl.schatz.jnomics.ob.SudoCollection;
import edu.cshl.schatz.jnomics.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * User: james
 * Base Class for Aligners
 * Does the work of writing the fastq files to disk and allows hooks for
 * launching aligners
 */
public abstract class AlignmentBaseMap
        extends JnomicsMapper<Writable, NullWritable, AlignmentCollectionWritable, NullWritable> {
    
    private static final JnomicsArgument readsBinArg = new JnomicsArgument("reads_per_bin",
            "Number of reads to align at a time",false);

    private int readsPerBin;
    private final File[] tmpFiles = new File[2];
    private boolean paired = false;

    
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
        return new JnomicsArgument[]{readsBinArg};
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        readsPerBin = conf.getInt(readsBinArg.getName(),10000);
        tmpFiles[0] = new File(context.getTaskAttemptID()+".1.fq");
        tmpFiles[1] = new File(context.getTaskAttemptID()+".2.fq");
        FileUtil.markDeleteOnExit(tmpFiles);
    }

    public boolean isPairedEnd(){
        return paired;
    }

    protected File[] getTempFiles(){
        return tmpFiles;
    }
    
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        System.out.println("Tmp Files----->");
        System.out.println(tmpFiles[0]);
        System.out.println(tmpFiles[1]);
        System.out.println("-----<");
        SudoCollection<FastqStringProvider> collection;
        BufferedOutputStream tmpWriter1 = new BufferedOutputStream(new FileOutputStream(tmpFiles[0]));
        BufferedOutputStream tmpWriter2 = new BufferedOutputStream(new FileOutputStream(tmpFiles[1]));
        int numReads = 0;
        while(context.nextKeyValue()){
            if(numReads++ >= readsPerBin){
                tmpWriter1.close();
                tmpWriter2.close();
                align(context);
                context.progress();
                FileUtil.removeFilesIfExist(tmpFiles);
                tmpWriter1 = new BufferedOutputStream(new FileOutputStream(tmpFiles[0]));
                tmpWriter2 = new BufferedOutputStream(new FileOutputStream(tmpFiles[1]));
                numReads = 0;
            }
            collection = (SudoCollection<FastqStringProvider>) context.getCurrentKey();
            tmpWriter1.write(collection.get(0).getFastqString().getBytes());
            tmpWriter1.write("\n".getBytes());
            if(collection.size() == 2){
                paired=true;
                tmpWriter2.write(collection.get(1).getFastqString().getBytes());
                tmpWriter2.write("\n".getBytes());
            }
        }
        tmpWriter1.close();
        tmpWriter2.close();
        align(context);
        cleanup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        FileUtil.removeFiles(tmpFiles);
    }

    protected abstract void align(Context context) throws IOException, InterruptedException;

}
