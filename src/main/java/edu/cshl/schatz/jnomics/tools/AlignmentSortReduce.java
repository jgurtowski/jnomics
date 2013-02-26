package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * User: Piyush Kansal
 */
public class AlignmentSortReduce
        extends JnomicsReducer <SamtoolsMap.SamtoolsKey,SAMRecordWritable,NullWritable,NullWritable> {

    public static final String UNMAPPED = "UNMAPPED";
    
    private FileSystem ipFs;

    @Override
    public Class getOutputKeyClass() {
        return NullWritable.class;
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
        return new JnomicsArgument[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    protected void setup( Context context ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        ipFs = FileSystem.get( conf );
    }

    protected void reduce( SamtoolsMap.SamtoolsKey key, final Iterable<SAMRecordWritable> values,
                           final Context context ) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        
        String chr = key.getRef().toString();
        if(chr.startsWith("*"))
            chr = UNMAPPED;
        Path outPath = FileOutputFormat.getWorkOutputPath(context);
        Path outFile = new Path(outPath, new String(chr+"-"+key.getBin()));

        SequenceFile.Writer writer = null;
        if(conf.get("mapred.output.compress","").compareTo("true") == 0){
            String codec_str = conf.get("mapred.output.compression.codec",
                    "org.apache.hadoop.io.compress.GzipCodec");
            CompressionCodec codec = null;
            try {
                codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(codec_str), conf);
            } catch (ClassNotFoundException e) {
                throw new IOException(e.toString());
            }
            writer = SequenceFile.createWriter(ipFs,conf,outFile,SAMRecordWritable.class,NullWritable.class,
                    SequenceFile.CompressionType.BLOCK, codec);
        }else{//no compression
            writer = SequenceFile.createWriter(ipFs,conf,outFile,SAMRecordWritable.class,NullWritable.class);
        }

        for(SAMRecordWritable w: values){
            writer.append(w,NullWritable.get());
        }

        writer.close();
    }
}
