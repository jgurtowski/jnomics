package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.AlignmentReaderContextWriter;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;


/**
 * Runs Bowtie2 Mapping process. Input must be ReadCollectionWritable
 */

public class Bowtie2Map extends AlignmentBaseMap{

    private String cmdPairedEnd,cmdSingleEnd;
    private AlignmentReaderContextWriter reader = null;

    private final JnomicsArgument bowtie_opts = new JnomicsArgument("bowtie_opts", "bowtie options",false);
    private final JnomicsArgument bowtie_idx = new JnomicsArgument("bowtie_index", "bowtie index",true);
    private final JnomicsArgument bowtie_binary = new JnomicsArgument("bowtie_binary", "bowtie binary",true);

    @Override
    public JnomicsArgument[] getArgs(){
        JnomicsArgument[] superArgs = super.getArgs();
        JnomicsArgument[] newArgs = new JnomicsArgument[]{bowtie_opts,bowtie_idx,bowtie_binary};
        return ArrayUtils.addAll(superArgs, newArgs);
    }

    @Override
    protected void setup(final Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        String bowtie_opts_str = context.getConfiguration().get(bowtie_opts.getName());
        bowtie_opts_str = bowtie_opts_str == null ? "" : bowtie_opts_str;
        String bowtie_idx_str = context.getConfiguration().get(bowtie_idx.getName());
        String bowtie_binary_str = context.getConfiguration().get(bowtie_binary.getName());
        if(!new File(bowtie_binary_str).exists())
            throw new IOException("Can't find bowtie binary: " + bowtie_binary_str);

        File[] tmpFiles = getTempFiles();

        cmdPairedEnd = String.format(
                "%s %s --mm -x %s -1 %s -2 %s",
                bowtie_binary_str, bowtie_opts_str, bowtie_idx_str,
                tmpFiles[0],
                tmpFiles[1]);

        cmdSingleEnd = String.format("%s %s --mm -x %s -U %s",
                bowtie_binary_str,
                bowtie_opts_str,
                bowtie_idx_str,
                tmpFiles[0]);
    }

    @Override
    public void align(final Context context) throws IOException, InterruptedException {
        if(null == reader)
            reader = new AlignmentReaderContextWriter(isPairedEnd());
        System.err.println("Starting bowtie2 Process");
        Process bowtieProcess;
        if(isPairedEnd()){
            bowtieProcess = Runtime.getRuntime().exec(cmdPairedEnd);
            System.out.println(cmdPairedEnd);
        }else{
            bowtieProcess = Runtime.getRuntime().exec(cmdSingleEnd);
            System.out.println(cmdSingleEnd);
        }
        Thread bowtieProcessErrThread = new Thread(
                new ThreadedStreamConnector(bowtieProcess.getErrorStream(),System.err)
        );

        bowtieProcessErrThread.start();

        /** Read lines from bowtie stdout and print them to context **/
        reader.read(bowtieProcess.getInputStream(),context);

        bowtieProcess.waitFor();
        bowtieProcessErrThread.join();
    }
}