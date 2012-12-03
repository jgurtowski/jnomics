package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.ArrayList;
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
    
    
    private Text blastTabOutput = new Text();

    private List<ReadWritable> reads;
    private int reads_per_bin;

    private int DEFAULT_READS_PER_BIN = 1000;
    
    
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
        JnomicsArgument[] newArgs = new JnomicsArgument[]{blast_opts,blast_idx,blast_binary,reads_per_bin_arg};
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

        //blastn -db -outfmt "6 std btop"
        blastCmd = String.format("%s -outfmt '6 std btop' -db %s ", blast_binary_str, blast_idx_str);
        System.out.println("Running: "+ blastCmd);
        reads = new ArrayList<ReadWritable>();
        reads_per_bin = conf.getInt(reads_per_bin_arg.getName(), DEFAULT_READS_PER_BIN);
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
                        try {
                            while((line = reader.readLine()) != null){
                                blastTabOutput.set(line);
                                context.progress();
                                try{
                                    context.write(blastTabOutput,NullWritable.get());
                                }catch(Exception e){
                                    System.err.println("Problem writing to context " + e.toString());
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
    }
}