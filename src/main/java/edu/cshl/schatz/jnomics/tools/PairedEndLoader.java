package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.FastqParser;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import edu.cshl.schatz.jnomics.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * User: james
 */
public class PairedEndLoader{

    final Logger logger = LoggerFactory.getLogger(PairedEndLoader.class);

    private ReadCollectionWritable key = new ReadCollectionWritable();
    private ReadWritable r1= new ReadWritable();
    private ReadWritable r2 = new ReadWritable();
    private Text keyName = new Text();
    private NullWritable value = NullWritable.get();

    public PairedEndLoader(){
        key.addRead(r1);
        key.addRead(r2);
        key.setName(keyName);
    }

    private SequenceFile.Writer getWriter(FileSystem fs, Path output) throws Exception {
        Configuration conf = getConf();
        SequenceFile.Writer writer;
        if(conf.get("mapred.output.compress","").compareTo("true") == 0){
            String codec_str = conf.get("mapred.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec");
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(codec_str), conf);
            writer = SequenceFile.createWriter(fs,conf,output,key.getClass(),value.getClass(),
                    SequenceFile.CompressionType.BLOCK, codec);
        }else{//no compression
            writer = SequenceFile.createWriter(fs,conf,output,key.getClass(),value.getClass());
        }
        
        logger.info("Compressed with:" + writer.getCompressionCodec() );

        return writer;
    }
    
    
    /**
     *
     * @param in1 Input stream for first read pair file
     * @param in2 Input stream for second read pair file
     * @param hdfsOut Output path for paired-end file in hdfs
     * @throws Exception
     */
    public void load(InputStream in1, InputStream in2, Path hdfsOut, FileSystem fs) throws Exception {

        FastqParser parser1 = new FastqParser(in1);
        FastqParser parser2 = new FastqParser(in2);

        if(fs.exists(hdfsOut)){
            throw new Exception("File already exists "+ hdfsOut);
        }

        Iterator<FastqParser.FastqRecord> it1 = parser1.iterator();
        Iterator<FastqParser.FastqRecord> it2 = parser2.iterator();

        FastqParser.FastqRecord record1,record2;

        SequenceFile.Writer writer = getWriter(fs,hdfsOut);

        int counter = 1;
        while(it1.hasNext() && it2.hasNext()){
            record1 = it1.next();
            record2 = it2.next();
            keyName.set(Integer.toString(counter));
            r1.setAll(record1.getName(), record1.getSequence(), record1.getDescription(), record1.getQuality());
            r2.setAll(record2.getName(),record2.getSequence(),record2.getDescription(),record2.getQuality());
            writer.append(key,value);
            counter++;
            if(counter % 100000 == 0){
                logger.info(Integer.toString(counter));
                progress();
            }
        }
        parser1.close();
        parser2.close();
        writer.close();
    }

    /**
     * Load interleaved fastq file
     * @param input1 - stream to read fastq data from
     * @param output - Path in hdfs to output the data
     * @param fs - filesystem used to output data
     */
    public void load(InputStream input1, Path output, FileSystem fs) throws Exception {
        FastqParser parser1 = new FastqParser(input1);

        if(fs.exists(output)){
            throw new Exception("File already exists "+ output);
        }

        Iterator<FastqParser.FastqRecord> it1 = parser1.iterator();

        SequenceFile.Writer writer = getWriter(fs,output);

        int counter = 1;
        
        while(it1.hasNext()){
            keyName.set(Integer.toString(counter));
            FastqParser.FastqRecord record1 =it1.next();
            r1.setAll(record1.getName(), record1.getSequence(), record1.getDescription(), record1.getQuality());
            if(!it1.hasNext()){
                throw new Exception("File does not have an even number of records");
            }
            FastqParser.FastqRecord record2 = it1.next();
            r2.setAll(record2.getName(), record2.getSequence(), record2.getDescription(), record2.getQuality());
            writer.append(key,value);
            counter++;
            if(counter % 100000 == 0){
                logger.info(Integer.toString(counter));
                progress();
            }
        }
        parser1.close();
        writer.close();
    }
    
    /**
     * Allows the task to report progress
     * Override to implement own progress hook
     */
    protected void progress(){
    }

    /**
     * Get a new configuration, or overload
     * @return Configuration
     */
    protected Configuration getConf(){
        return new Configuration();
    }
    

    public static void main(String[] args) throws Exception {

        if(args.length != 3 && args.length != 4 ){
            throw new Exception("Usage: " + PairedEndLoader.class + " <in.1.fq> <in.2.fq> output [hdfs://namenode:port/]");
        }

        File file1 = new File(args[0]);
        File file2 = new File(args[1]);
        Path out = new Path(args[2]+".pe");

        InputStream in1 = FileUtil.getInputStreamWrapperFromExtension(new FileInputStream(file1),
                FileUtil.getExtension(file1.getName()));
        InputStream in2 = FileUtil.getInputStreamWrapperFromExtension(new FileInputStream(file2),
                FileUtil.getExtension(file2.getName()));

        final Configuration conf = new Configuration();

        if(args.length == 3){
            FileSystem fs = FileSystem.get(conf);
            new PairedEndLoader().load(in1,in2,out,fs);
        }else{
            conf.set("fs.default.name",args[3]);
            FileSystem fs = FileSystem.get(conf);
            new PairedEndLoader().load(in1,in2,out,fs);
        }
    }

    public void runTask(String[] args) throws Exception{
        PairedEndLoader.main(args);
    }
}
