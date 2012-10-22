/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.mapreduce;

import java.io.IOException;

/**
 * @author Matthew Titmus
 */
public class UnknownReadFileFormatException extends IOException {
    /**
     * 
     */
    public UnknownReadFileFormatException() {}

    /**
     * @param message
     */
    public UnknownReadFileFormatException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public UnknownReadFileFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param cause
     */
    public UnknownReadFileFormatException(Throwable cause) {
        super(cause);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

    }
}
