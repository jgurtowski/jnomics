/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.io;

import java.io.IOException;

/**
 * @author Matthew Titmus
 */
public class ReadFormatException extends IOException {
    /**
	 * 
	 */
    public ReadFormatException() {}

    /**
     * @param message
     */
    public ReadFormatException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public ReadFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param cause
     */
    public ReadFormatException(Throwable cause) {
        super(cause);
    }
}
