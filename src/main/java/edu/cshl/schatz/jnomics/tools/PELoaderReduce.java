package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.FastqParser;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.ob.writable.PEMetaInfo;
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
import java.util.Iterator;
import java.util.Map;

/**
 * User: james
 */
public class PELoaderReduce extends JnomicsReducer<IntWritable, PEMetaInfo, Text, NullWritable> {

    final Logger logger = LoggerFactory.getLogger(PELoaderReduce.class);

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
    protected void reduce(IntWritable key, Iterable<PEMetaInfo> values, final Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();

        for (PEMetaInfo seInfo : values) {

            Path p1 = new Path(seInfo.getFirstFile());
            Path p2 = new Path(seInfo.getSecondFile());
            logger.info(seInfo.getFirstFile());
            logger.info(seInfo.getSecondFile());
            System.out.println(seInfo.getFirstFile());
            System.out.println(seInfo.getSecondFile());
            context.write(new Text(p1.toString()),NullWritable.get());
            context.write(new Text(p2.toString()),NullWritable.get());

            logger.info("Opening File Streams");
            FileSystem fileSystem = FileSystem.get(p1.toUri(),conf);
            InputStream in1 = FileUtil.getInputStreamWrapperFromExtension(fileSystem.open(p1),
                    FileUtil.getExtension(p1.getName()));
            InputStream in2 = FileUtil.getInputStreamWrapperFromExtension(fileSystem.open(p2),
                    FileUtil.getExtension(p2.getName()));

            logger.info("Opening outfile path, removing if exists");
            Path outPath = new Path(seInfo.getDestination());
            if(fileSystem.exists(outPath)) //if process fails and is rescheduled, remove old broken files
                fileSystem.delete(outPath,false);

            NullWritable sfValue = NullWritable.get();
            ReadCollectionWritable sfKey  = new ReadCollectionWritable();

            ReadWritable r1= new ReadWritable();
            ReadWritable r2= new ReadWritable();
            sfKey.addRead(r1);
            sfKey.addRead(r2);
            Text keyName = new Text();
            sfKey.setName(keyName);


            
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

            FastqParser parser1 = new FastqParser(in1);
            FastqParser parser2 = new FastqParser(in2);

            Iterator<FastqParser.FastqRecord> i1 = parser1.iterator();
            Iterator<FastqParser.FastqRecord> i2 = parser2.iterator();

            logger.info("Writing to SequenceFile");
            FastqParser.FastqRecord record1,record2;
            long count=0;
            while(i1.hasNext() && i2.hasNext()){
                record1 = i1.next();
                record2 = i2.next();
                r1.setAll(record1.getName(),record1.getSequence(),record1.getDescription(),record1.getQuality());
                r2.setAll(record2.getName(),record2.getSequence(),record2.getDescription(),record2.getQuality());
                keyName.set(record1.getName());
                writer.append(sfKey, sfValue);
                if(0 == ++count % 1000000){
                    context.write(new Text(Long.toString(count)),NullWritable.get());
                    System.out.println(count);
                    context.progress();
                }
            }
            logger.info("Done writing");

            parser1.close();
            parser2.close();
            writer.close();

            logger.info("Cleaning up");
        }
    }
}
