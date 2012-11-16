package edu.cshl.schatz.jnomics.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * User: james
 */
public class DefaultInputStreamHandler implements InputStreamHandler{

    private OutputStream os;

    private byte[] bytes = new byte[1024];

    /**
     * @param os The stream to write the input to
     */
    public DefaultInputStreamHandler(OutputStream os){
        this.os = os;
    }

    @Override
    public void handle(InputStream in) {
        try{
            while(in.read(bytes) > 0){
                os.write(bytes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
