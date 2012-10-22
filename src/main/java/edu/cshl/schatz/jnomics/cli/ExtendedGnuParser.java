package edu.cshl.schatz.jnomics.cli;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;

import java.util.ListIterator;

/**
 * User: james
 * Code hint from:
 * http://stackoverflow.com/questions/6049470/can-apache-commons-cli-options-parser-ignore-unknown-command-line-options
 */


public class ExtendedGnuParser extends GnuParser {

    private boolean ignoreUnrecognizedOption;

    public ExtendedGnuParser(final boolean ignoreUnrecognizedOption) {
        this.ignoreUnrecognizedOption = ignoreUnrecognizedOption;
    }

    @Override
    protected void processOption(final String arg, final ListIterator iter) throws ParseException {
        boolean hasOption = getOptions().hasOption(arg);

        if (hasOption || !ignoreUnrecognizedOption) {
            super.processOption(arg, iter);
        }
    }

}