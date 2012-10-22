package edu.cshl.schatz.jnomics.util.functional;

/**
 *james
 */
public abstract class Operation {

    public <T> void performOperation(T data) throws Exception{
        throw new Exception("Override this method");
    }
    
    public <T,R> R performAndReturnOperation(T data) throws Exception{
        throw new Exception("Override this method");
    }

}
