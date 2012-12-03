package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
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

/**
 * Created by IntelliJ IDEA.
 */
public class SequenceLoaderBase {

    protected ReadCollectionWritable key = new ReadCollectionWritable();
    protected NullWritable value = NullWritable.get();

    final Logger logger = LoggerFactory.getLogger(SequenceLoaderBase.class);

    protected SequenceFile.Writer getWriter(FileSystem fs, Path output) throws Exception {
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
     * Get a new configuration, or overload
     * @return Configuration
     */
    protected Configuration getConf(){
        return new Configuration();
    }

    /**
     * Allows the task to report progress
     * Override to implement own progress hook
     */
    protected void progress(){
    }

}