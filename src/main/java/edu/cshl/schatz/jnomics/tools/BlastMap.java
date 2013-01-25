package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Runs Blast Mapping process. Input must be ReadCollectionWritable
 */

public class BlastMap extends JnomicsMapper<ReadCollectionWritable, NullWritable, Text,NullWritable>{

    private String blastCmd = null;

    private final JnomicsArgument blast_opts = new JnomicsArgument("blast_opts", "blast options",false);
    private final JnomicsArgument blast_idx = new JnomicsArgument("blast_index", "blast index",true);
    private final JnomicsArgument blast_binary = new JnomicsArgument("blast_binary", "blast binary",true);
    private final JnomicsArgument reads_per_bin_arg = new JnomicsArgument("reads_per_bin","Number of reads aligned at once", false);
    private final JnomicsArgument fake_progress_arg = new JnomicsArgument("fake_progress","Fake progress so that it does not time out [true/false]", false);
    
    private Text blastTabOutput = new Text();

    private List<ReadWritable> reads;
    private int reads_per_bin;

    private static final int DEFAULT_READS_PER_BIN = 1000;

    //interval at which progress is reported
    private static final int FAKE_PROGRESS_INTERVAL = 30000;
    
    private FSDataOutputStream unalignedOut;

    private boolean fake_progress;
    
    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs(){
        JnomicsArgument[] newArgs = new JnomicsArgument[]{blast_opts,blast_idx,blast_binary,
                reads_per_bin_arg,fake_progress_arg};
        return newArgs;
    }

    @Override
    protected void setup(final Context context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        String blast_opts_str = conf.get(blast_opts.getName());
        blast_opts_str = blast_opts_str == null ? "" : blast_opts_str;
        String blast_idx_str = conf.get(blast_idx.getName());
        String blast_binary_str = conf.get(blast_binary.getName());
        if(!new File(blast_binary_str).exists())
            throw new IOException("Can't find Blast binary: " + blast_binary_str);
        
        fake_progress = conf.getBoolean(fake_progress_arg.getName(),false);
        
        //blastn -db -outfmt "6 std btop"
        blastCmd = String.format("%s %s -outfmt '6 std btop' -db %s ", blast_binary_str,blast_opts_str,blast_idx_str);
        System.out.println("Running: "+ blastCmd);
        reads = new ArrayList<ReadWritable>();
        reads_per_bin = conf.getInt(reads_per_bin_arg.getName(), DEFAULT_READS_PER_BIN);

        String task_attempt = context.getTaskAttemptID().toString();
        FileSystem fs = FileSystem.get(conf);
        unalignedOut = fs.create(FileOutputFormat.getPathForWorkFile(context,"_unaligned/"+task_attempt,".unmapped"));
    }

    @Override
    public void run(final Context context) throws IOException, InterruptedException {
        setup(context);
        reads.clear();
        while(context.nextKeyValue()){
            reads.add(context.getCurrentKey().getRead(0));
            if(reads.size() >= reads_per_bin){
                align(reads, context);
                reads.clear();
            }
        }
        if(reads.size() > 0)
            align(reads,context);
        reads.clear();
    }

    private void align(final List<ReadWritable> reads, final Context context) throws IOException {
        System.out.println("Aligning " + reads.size() + " reads");

        //spawn a thread to fake progress every FAKE_PROGRESS_INTERVAL
        Thread t = null;
        if(fake_progress){
            t = new Thread(new Runnable() {
                @Override
                public void run() {
                    while(true){
                        try {
                            Thread.sleep(FAKE_PROGRESS_INTERVAL);
                            context.progress();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }
            });
            t.start();
            System.out.println("Starting fake progress at intervals of " + FAKE_PROGRESS_INTERVAL);
        }

        ProcessUtil.runCommandEZ(new Command(blastCmd,
                //push reads through blast's stdin (fasta format)
                new OutputStreamHandler() {
                    @Override
                    public void handle(OutputStream out) {
                        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
                        for(ReadWritable read : reads){
                            try{
                                context.progress();
                                writer.write(new String(">"+read.getName().toString()+"\n"));
                                writer.write(new String(read.getSequence().toString()+"\n"));
                            }catch(Exception e){
                                System.err.println("Error Writing to Blast Input Stream " + e.toString());
                            }
                        }
                        try{
                            writer.close();
                        }catch(Exception e){
                            System.err.println("Couldn't close blast's stdin stream");
                        }                        
                    }
                },
                //blast stdout gets saved to output file
                new InputStreamHandler() {
                    @Override
                    public void handle(InputStream in) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                        String line = null;
                        HashMap<String,Boolean> alignedReads = new HashMap<String, Boolean>();
                        try {
                            while(null != (line = reader.readLine())){
                                alignedReads.put(line.split("\t")[0], true);
                                blastTabOutput.set(line);
                                context.progress();
                                try{
                                    context.write(blastTabOutput,NullWritable.get());
                                }catch(Exception e){
                                    System.err.println("Problem writing to context " + e.toString());
                                }
                            }

                            //write the unaligned reads to separate file
                            for(ReadWritable r: reads){
                                String groomedName = r.getName().toString().split("\\s+")[0];
                                if(!alignedReads.containsKey(groomedName)){
                                    unalignedOut.write(r.getFastqString().getBytes());
                                    unalignedOut.write("\n".getBytes());
                                }
                            }

                        } catch (IOException e) {
                            System.err.println("Error reading from Blast stdout " + e.toString() );
                        }
                    }
                },
                //blast stderr gets printed to hadoop stderr
                new DefaultInputStreamHandler(System.err)
        ));

        if(fake_progress){
            System.out.println("Interrupting fake progress thread");
            t.interrupt();
            try {
                t.join();
            } catch (InterruptedException e) {
            }
            System.out.println("fake progress thread is done");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        unalignedOut.close();
    }
}