package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.FastqParser;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.ob.writable.SEMetaInfo;
import edu.cshl.schatz.jnomics.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * User: james
 */
public class SELoaderReduce extends JnomicsReducer<IntWritable, SEMetaInfo, Text, NullWritable> {

    final Logger logger = LoggerFactory.getLogger(SELoaderReduce.class);

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
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
    protected void reduce(IntWritable key, Iterable<SEMetaInfo> values, final Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();

        for (SEMetaInfo seInfo : values) {
            logger.info("Working on : " + seInfo.getFile());
            System.out.println(seInfo.getFile());
            Path p1 = new Path(seInfo.getFile());

            context.write(new Text(p1.toString()),NullWritable.get());

            FileSystem fileSystem = FileSystem.get(p1.toUri(),conf);
            Path outPath = new Path(seInfo.getDestination());
            logger.info("Checking if "+outPath+" exists");
            if(fileSystem.exists(outPath)) //if process fails and is rescheduled, remove old broken files
                fileSystem.delete(outPath,false);

            logger.info("Opening input stream for file "+ p1);
            InputStream in1 = FileUtil.getInputStreamWrapperFromExtension(fileSystem.open(p1),
                    FileUtil.getExtension(p1.getName()));

            NullWritable sfValue = NullWritable.get();
            ReadCollectionWritable sfKey  = new ReadCollectionWritable();

            ReadWritable r1= new ReadWritable();
            sfKey.addRead(r1);
            Text keyName = new Text();
            sfKey.setName(keyName);

            logger.info("Building a SequenceFile writer");
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
                writer = SequenceFile.createWriter(fileSystem,conf,outPath,sfKey.getClass(),sfValue.getClass(),
                        SequenceFile.CompressionType.BLOCK, codec);
            }else{//no compression
                writer = SequenceFile.createWriter(fileSystem,conf,outPath,sfKey.getClass(),sfValue.getClass());
            }
            
            FastqParser parser = new FastqParser(in1);
            
            logger.info("Writing fastq records to SequenceFile");
            long count = 0;
            for(FastqParser.FastqRecord record: parser){
                r1.setAll(record.getName(),record.getSequence(),record.getDescription(),record.getQuality());
                keyName.set(record.getName());
                writer.append(sfKey,sfValue);
                if(0 == ++count % 100000){
                    context.write(new Text(Long.toString(count)),NullWritable.get());
                    context.progress();
                    System.out.println(count);
                }            
            }
            
            logger.info("Done Writing, cleaning up");
            
            parser.close();
            in1.close();
            writer.close();

            logger.info("Done Cleaning up");
        }
    }
}
