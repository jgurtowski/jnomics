package edu.cshl.schatz.jnomics.test;

import edu.cshl.schatz.jnomics.io.MPHeader;
import edu.cshl.schatz.jnomics.io.MPStreamWriter;
import edu.cshl.schatz.jnomics.util.ByteUtil;
import junit.framework.TestCase;
import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;
import org.msgpack.unpacker.Unpacker;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;


/**
 * User: james
 */
public class MPStreamWriterTest extends TestCase{

    @Message
    public static class MPTestObj{
        public String read1;
        public String read2;
        public int num;
    }
    
    public void testWriter() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        HashMap<String,String> hm = new HashMap<String,String>();
        hm.put("hello","Dude");
        MPStreamWriter writer = new MPStreamWriter(output,hm,1024);
        MPTestObj o = new MPTestObj();
        o.read1 = "ACAGGGGGGGGGGGGGGGGAGAGAGAGAG";
        o.read2 = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCC";
        o.num = 1;
        writer.addRecord(o);
        o.num = 2;
        writer.addRecord(o);
        o.num = 3;
        writer.addRecord(o);
        /*for(int i = 0; i < 10; i++){
            o.num = i;
            writer.addRecord(o);
        } */
        writer.close();

        ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

        Unpacker up = new MessagePack().createUnpacker(in);
        MPHeader header = up.read(MPHeader.class);
        String delim = header.delimiter;
        System.out.println(delim);
        System.out.println(Arrays.toString(delim.getBytes()));
        byte []c = new byte[delim.getBytes().length];
        in.read(c);
        System.out.println(Arrays.toString(c));
        System.out.println(Arrays.toString(output.toByteArray()));

        byte[] next = new byte[1];

        ByteArrayOutputStream tmp = new ByteArrayOutputStream();
        while(in.read(next) > 0){
            tmp.write(next[0]);
            if(ByteUtil.reverseEndEqual(tmp.toByteArray(), delim.getBytes())){
                byte[] compressed = tmp.toByteArray();
                byte[] uncompressed = new byte[1024 * 1024];
                int unc_size = Snappy.uncompress(compressed,0,compressed.length-delim.length(),uncompressed,0);
                Unpacker up2 = new MessagePack().createUnpacker(new ByteArrayInputStream(uncompressed));
                MPTestObj ol = up2.read(MPTestObj.class);
                System.out.println(ol.num);
                ol = up2.read(MPTestObj.class);
                System.out.println(ol.num);
                ol = up2.read(MPTestObj.class);
                System.out.println(ol.num);
                System.exit(0);
            }
        }
    }
}
