package edu.cshl.schatz.jnomics.util;

/**
 * User: james
 * Commandline command run through system call
 */
public class Command {

    private String command;
    private OutputStreamHandler inHandler;
    private InputStreamHandler stdoutHandler, stderrHandler;


    /**
     * @param command  Command to be run
     * @param in StreamHandler that will handle writing data to the stdin of the process
     * @param stdout StreamHandler that will handle reading data from stdout of the process
     * @param stderr StreamHandler that will handle reading data from stderr of the process
     */
    public Command(String command, OutputStreamHandler in, InputStreamHandler stdout, InputStreamHandler stderr){
        this.command = command;
        inHandler = in;
        stdoutHandler = stdout;
        stderrHandler = stderr;
    }

    public Command(String command){
        this(command,
                new DefaultOutputStreamHandler(),
                new DefaultInputStreamHandler(System.out),
                new DefaultInputStreamHandler(System.err));
    }
    
    public Command(String command, OutputStreamHandler in){
        this(command, in, new DefaultInputStreamHandler(System.out),new DefaultInputStreamHandler(System.err));
    }

    public Command(String command, InputStreamHandler stdout, InputStreamHandler stderr){
        this(command, new DefaultOutputStreamHandler(), stdout, stderr);
    }

    public OutputStreamHandler getInHandler() {
        return inHandler;
    }

    public InputStreamHandler getStdoutHandler() {
        return stdoutHandler;
    }

    public InputStreamHandler getStderrHandler() {
        return stderrHandler;
    }

    public String getCommand() {
        return command;
    }

}
