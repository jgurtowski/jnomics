package edu.cshl.schatz.jnomics.test;

import edu.cshl.schatz.jnomics.io.MPHeader;
import junit.framework.TestCase;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;

/**
 * User: james
 */
public class MPHeaderTest extends TestCase{

    public void testHeaderSerialization() throws IOException {

        MPHeader header = new MPHeader();
        header.compression_codec = "snappy";
        header.delimiter = "12345";
        header.userdata = new HashMap<String, String>();
        header.userdata.put("ello","joe");

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        MessagePack msgpack = new MessagePack();
        Packer packer = msgpack.createPacker(os);
        packer.write(header);

        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        Unpacker unpacker = msgpack.createUnpacker(is);
        MPHeader header2 = unpacker.read(MPHeader.class);

        System.out.println(header2.userdata.get("ello"));
        
    }


}
