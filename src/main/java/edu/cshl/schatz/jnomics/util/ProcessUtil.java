package edu.cshl.schatz.jnomics.util;

import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;

import java.io.IOException;

/**
 * User: james
 */
public class ProcessUtil {

    public static int execAndReconnect(String cmd) throws IOException, InterruptedException {
        System.out.println(cmd);
        Process p = Runtime.getRuntime().exec(cmd);
        Thread errConn = new Thread(new ThreadedStreamConnector(p.getErrorStream(),System.err));
        Thread outConn = new Thread(new ThreadedStreamConnector(p.getInputStream(),System.out));
        errConn.start();
        outConn.start();
        p.waitFor();
        errConn.join();
        outConn.join();
        return p.exitValue();
    }

    public static void exceptionOnError(int errorCode) throws IOException{
        if(0 != errorCode)
            throw new IOException(String.valueOf(errorCode));
    }
}
