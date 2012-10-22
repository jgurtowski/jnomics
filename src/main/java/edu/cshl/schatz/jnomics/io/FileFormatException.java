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
public class FileFormatException extends IOException {
    private static final long serialVersionUID = -6919567536520269531L;

    /**
	 * 
	 */
    public FileFormatException() {
        super();
    }

    /**
     * @param message
     */
    public FileFormatException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public FileFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param cause
     */
    public FileFormatException(Throwable cause) {
        super(cause);
    }
}
