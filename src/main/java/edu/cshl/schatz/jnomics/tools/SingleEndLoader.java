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
import java.io.InputStream;
import java.util.Iterator;

/**
 * User: james
 */
public class SingleEndLoader extends SequenceLoaderBase{

    final Logger logger = LoggerFactory.getLogger(SingleEndLoader.class);

    //key and value come from SequenceLoaderBase

    private ReadWritable r1= new ReadWritable();
    private Text keyName = new Text();

    public SingleEndLoader(){
        key.addRead(r1);
        key.setName(keyName);
    }

    /**
     *
     * @param in1 Input stream for first read pair file
     * @param hdfsOut Output path for paired-end file in hdfs
     * @throws Exception
     */
    public void load(InputStream in1, Path hdfsOut, FileSystem fs) throws Exception {

        FastqParser parser1 = new FastqParser(in1);

        if(fs.exists(hdfsOut)){
            throw new Exception("File already exists "+ hdfsOut);
        }

        Iterator<FastqParser.FastqRecord> it1 = parser1.iterator();

        FastqParser.FastqRecord record1;

        SequenceFile.Writer writer = getWriter(fs,hdfsOut);

        int counter = 1;
        while(it1.hasNext()){
            record1 = it1.next();
            keyName.set(Integer.toString(counter));
            r1.setAll(record1.getName(), record1.getSequence(), record1.getDescription(), record1.getQuality());
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


    public static void main(String[] args) throws Exception {

        if(args.length != 2 ){
            throw new Exception("Usage: " + SingleEndLoader.class + " <in.1.fq> output");
        }

        File file1 = new File(args[0]);
        Path out = new Path(args[1]+".se");

        InputStream in1 = FileUtil.getInputStreamWrapperFromExtension(new FileInputStream(file1),
                FileUtil.getExtension(file1.getName()));

        final Configuration conf = new Configuration();

        if(args.length == 2){
            FileSystem fs = FileSystem.get(conf);
            new SingleEndLoader().load(in1,out,fs);
        }else{
            conf.set("fs.default.name",args[2]);
            FileSystem fs = FileSystem.get(conf);
            new SingleEndLoader().load(in1,out,fs);
        }
    }

    public void runTask(String[] args) throws Exception{
        SingleEndLoader.main(args);
    }
}
