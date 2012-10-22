package edu.cshl.schatz.jnomics.io;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *James
 * Reads from inputstream writes to outputstream
 */
public class ThreadedStreamConnector implements Runnable {

    InputStream in;
    OutputStream out;
    
    public ThreadedStreamConnector(InputStream in, OutputStream out){
        this.in = in;
        this.out = out;
    }

    @Override
    public void run() {
        byte[] data = new byte[1024];
        int len;
        try{
            while((len = in.read(data)) != -1){
                out.write(data,0,len);
                out.flush();
                progress();
            }
            out.flush();
        }catch(Exception e){
            System.err.println(e);
        }
    }

    /**
     * Report progress, override hook
     */
    public void progress(){

    }

    public void closeOutputStream() throws IOException {
        out.close();
    }
    
    public void closeInputStream() throws IOException{
        in.close();
    }
}
