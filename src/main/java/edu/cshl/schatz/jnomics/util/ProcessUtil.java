package edu.cshl.schatz.jnomics.util;

import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;

import java.io.IOException;

/**
 * User: james
 */
public class ProcessUtil {

    /**
     * Run a command via System call.
     * @param command Command to be run
     * @return Exit code of child process
     * @throws Exception
     */

    public static int runCommand(final Command command) throws Exception {

        final Process process = Runtime.getRuntime().exec(command.getCommand());
        
        Thread pinThread = new Thread(new Runnable() {
            @Override
            public void run() {
                command.getInHandler().handle(process.getOutputStream());
            }
        });
        
        Thread poutThread = new Thread(new Runnable() {
            @Override
            public void run() {
                command.getStdoutHandler().handle(process.getInputStream());
            }
        });
        
        Thread perrThread = new Thread(new Runnable() {
            @Override
            public void run() {
                command.getStderrHandler().handle(process.getErrorStream());
            }
        });

        pinThread.start();
        poutThread.start();
        perrThread.start();
        
        int status = process.waitFor();
        pinThread.join();
        poutThread.join();
        perrThread.join();

        return status;
    }

    /**
     * Run a Commandline command, but return an exception if there is a Non-Zero Exit status
     * @param command  Command to run
     * @throws Exception
     */
    public static void runCommandEZ(Command command) throws IOException {
        int status = -1;
        try{
            status = runCommand(command);
        }catch(Exception e){
            throw new IOException(e);
        }
        if(status != 0){
            throw new IOException("Command returned non-zero exits status: " + command.getCommand());
        }
    }
    

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
