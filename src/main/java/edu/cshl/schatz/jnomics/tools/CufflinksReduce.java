package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * User: james
 */
public class CufflinksReduce extends JnomicsReducer<SamtoolsMap.SamtoolsKey,SAMRecordWritable,NullWritable,NullWritable> {
    
    private final JnomicsArgument cuff_binary_arg = new JnomicsArgument("cufflinks_binary","Cufflinks Binary",true);

    private FileSystem fs;
    
    private File tmpFile,tmpOutputDir;
    
    private String cufflinks_cmd;
    
    @Override
    public Class getOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{cuff_binary_arg};
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
        String cuff_binary_str = conf.get(cuff_binary_arg.getName());
        if(!new File(cuff_binary_str).exists())
            throw new IOException("Cannot find Cufflinks binary");

        String taskID = context.getTaskAttemptID().toString();
        tmpFile = new File(taskID+".sam");
        tmpOutputDir = new File(taskID+"-out");
        cufflinks_cmd = String.format("%s -o %s %s", cuff_binary_str, tmpOutputDir, tmpFile);
        fs = FileSystem.get(conf);
    }

    @Override
    protected void reduce(SamtoolsMap.SamtoolsKey key, Iterable<SAMRecordWritable> values, final Context context)
            throws IOException, InterruptedException {

        BufferedOutputStream tmpWriter = new BufferedOutputStream(new FileOutputStream(tmpFile));

        long count = 0;
        for(SAMRecordWritable read: values){
            tmpWriter.write(read.toString().getBytes());
            tmpWriter.write("\n".getBytes());
            if(0 == ++count % 1000){
                context.progress();
                context.write(NullWritable.get(),NullWritable.get());
            }
        }
        tmpWriter.close();

        Process process = Runtime.getRuntime().exec(cufflinks_cmd);

        Thread connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),System.out));
        Thread connecterr = new Thread(new ThreadedStreamConnector(process.getErrorStream(),System.err){
            @Override
            public void progress() {
                context.progress();
            }
        });
        connectout.start();
        connecterr.start();
        connectout.join();
        connecterr.join();
        process.waitFor();

        Configuration conf = context.getConfiguration();
        String outdir = conf.get("mapred.output.dir");

        String[] localFiles = new String[]{"transcripts.gtf","skipped.gtf",
                "isoforms.fpkm_tracking","genes.fpkm_tracking"};

        for(String fileStr : localFiles){
            fs.copyFromLocalFile(new Path(tmpOutputDir+"/"+fileStr),
                    new Path(outdir+"/"+key.getRef()+"-"+fileStr));
        }

        //cleanup
        tmpFile.delete();
        //Files.deleteRecursively(tmpOutputDir);
    }
}
