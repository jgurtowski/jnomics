package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import edu.cshl.schatz.jnomics.ob.writable.SEMetaInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;

/**
 * User: james
 */
public class HttpLoaderReduce extends JnomicsReducer<IntWritable, SEMetaInfo, Text, NullWritable> {

    private byte []buffer = new byte[10240];
    private FileSystem fs;
    
    private static final JnomicsArgument proxy_arg = new JnomicsArgument("proxy","http proxy", false);
    private String proxy;
    
    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{proxy_arg};
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fs = FileSystem.get(conf);
        proxy = conf.get(proxy_arg.getName(),null);
        if(null != proxy){
            String []arr = proxy.split(":");
            if(2 != arr.length)
                throw new InterruptedException("Proxy in bad form should be in form ip:port");
            System.setProperty("http.proxyHost",arr[0]);
            System.setProperty("http.proxyPort",arr[1]);
        }
    }

    @Override
    protected void reduce(IntWritable key, Iterable<SEMetaInfo> values, final Context context)
            throws IOException, InterruptedException {

        //OutputStream outputStream = null;
        InputStream is;
        for(SEMetaInfo info: values){
            System.out.println("Downloading file: " + info.getFile());
            URL url = new URL(info.getFile());
            is = url.openStream();
            //outputStream = fs.create(new Path(info.getDestination()),false);
            try {
                new PairedEndLoader(){
                    @Override
                    protected void progress() {
                        context.progress();
                    }
                    @Override
                    protected Configuration getConf() {
                        return context.getConfiguration();
                    }
                }.load(is,new Path(info.getDestination()),fs);
            } catch (Exception e) {
                throw new IOException("Unable to write file");
            }

            /*
            while((read = is.read(buffer)) > 0){
                outputStream.write(buffer,0,read);
                context.progress();
            }*/
            is.close();
            context.write(new Text("Completed Downloading file: " + info.getFile()),NullWritable.get());
        }
    }

}
