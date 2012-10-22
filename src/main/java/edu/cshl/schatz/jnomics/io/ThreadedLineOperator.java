package edu.cshl.schatz.jnomics.io;

import edu.cshl.schatz.jnomics.util.functional.Operation;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
  * User: james
  * Performs operation on every line in a stream using a thread
  */
public class ThreadedLineOperator implements Runnable {

    private InputStream in;
    private Operation op;
    
    public ThreadedLineOperator(InputStream in, Operation op ){
        this.in = in;
        this.op = op;
    }

    @Override
    public void run() {
        BufferedReader stream = new BufferedReader(new InputStreamReader(in));
        String line;
        try{
            while((line = stream.readLine()) != null){
                op.performOperation(line);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
