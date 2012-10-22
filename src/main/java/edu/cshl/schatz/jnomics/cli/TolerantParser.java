/*
 * This file is part of Jnomics.core.
 * Copyright 2011 Matthew A. Titmus
 * All rights reserved.
 *  
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *       
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *       
 *     * Neither the name of the Cold Spring Harbor Laboratory nor the names of
 *       its contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package edu.cshl.schatz.jnomics.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;

/**
 * An extension of the standard Apache CLI GnuParser that doesn't throw an
 * exception when it sees an unknown argument.
 * <p>
 * When the standard
 * {@link #parse(org.apache.commons.cli.Options, String[], boolean)} method
 * finds an option that isn't known, it either quietly quite (when
 * <code>stopAtNonOption==true</code>) or throws an
 * {@link UnrecognizedOptionException}. When this class, however, doesn't
 * recognize an exception (and <code>stopAtNonOption==false</code>), it quietly
 * adds it to its {@link CommandLine}'s "unknown args" list, accessible via
 * {@link CommandLine#getArgs()}. It <i>does not </i>throw an exception.
 * 
 * @author Matthew A. Titmus
 */
public class TolerantParser extends GnuParser {
    @SuppressWarnings("rawtypes")
    private List newCommandLineArgs = new ArrayList();

    static String stripLeadingHyphens(String str) {
        if (str == null) {
            return null;
        }

        char[] chars = str.toCharArray();

        for (int i = 0; i < chars.length; i++) {
            if (chars[i] != '-') {
                return str.substring(i);
            }
        }

        return str;
    }

    /*
     * @see org.apache.commons.cli.Parser#parse(org.apache.commons.cli.Options,
     * java.lang.String[], java.util.Properties, boolean)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public CommandLine parse(Options options, String[] arguments, Properties properties,
        boolean stopAtNonOption) throws ParseException {

        if (options == null) {
            options = new Options();
        }
        
        CommandLine cli = super.parse(options, arguments, properties, stopAtNonOption);

        List commandLineArgs = cli.getArgList();

        commandLineArgs.clear();
        commandLineArgs.addAll(newCommandLineArgs);
        return cli;
    }

    /**
     * This flatten method does so using the following rules:
     * <ol>
     * <li>If an {@link Option} exists for the first character of the
     * <code>arguments</code> entry <b>AND</b> an {@link Option} does not exist
     * for the whole <code>argument</code> then add the first character as an
     * option to the processed tokens list e.g. "-D" and add the rest of the
     * entry to the also.</li>
     * <li>Otherwise just add the token to the processed tokens list.</li>
     * </ol>
     * 
     * @param options The Options to parse the arguments by.
     * @param arguments The arguments that have to be flattened.
     * @param stopAtNonOption specifies whether to stop flattening when a non
     *            option has been encountered
     * @return a String array of the flattened arguments
     */
    @SuppressWarnings("unchecked")
    @Override
    protected String[] flatten(Options options, String[] arguments, boolean stopAtNonOption) {
        List<String> knownTokens = new ArrayList<String>();
        List<String> unknownTokens = new ArrayList<String>();
        List<String> tokens = knownTokens;

        int numArgs = 0;
        boolean eatTheRest = false;

        for (int i = 0; i < arguments.length; i++) {
            String arg = arguments[i];

            if ("--".equals(arg)) {
                eatTheRest = true;
                (tokens = knownTokens).add("--");
            } else if ("-".equals(arg)) {
                (tokens = knownTokens).add("-");
            } else if (arg.contains(" ")) {
                // Arg contains a space; it's not an option. Assume it's a
                // quote-enclosed argument.
                tokens.add(arg);
            } else if (arg.startsWith("-")) {
                String opt = stripLeadingHyphens(arg);

                if (options.hasOption(opt)) {
                    numArgs = options.getOption(opt).getArgs();
                    
                    if (Option.UNLIMITED_VALUES == numArgs) {
                        numArgs = Integer.MAX_VALUE;
                    }

                    (tokens = knownTokens).add(arg);
                } else {
                    if ((opt.indexOf('=') != -1)
                            && options.hasOption(opt.substring(0, opt.indexOf('=')))) {
                        // the format is --foo=value or -foo=value
                        (tokens = knownTokens).add(arg.substring(0, arg.indexOf('='))); // --foo
                        (tokens = knownTokens).add(arg.substring(arg.indexOf('=') + 1)); // value
                    } else {
                        eatTheRest = stopAtNonOption;
                        (tokens = unknownTokens).add(arg);
                    }
                }
            } else if (numArgs-- > 0) {
                // Add to the last used token list.
                tokens.add(arg);
            } else {
                unknownTokens.add(arg);
            }

            if (eatTheRest) {
                for (i++; i < arguments.length; i++) {
                    unknownTokens.add(arguments[i]);
                }
            }
        }

        newCommandLineArgs.clear();
        newCommandLineArgs.addAll(unknownTokens);

        knownTokens.addAll(unknownTokens);

        return knownTokens.toArray(new String[knownTokens.size()]);
    }

    /**
     * Process the Option specified by <code>arg</code> using the values
     * retrieved from the specfied iterator <code>iter</code>.
     * 
     * @param arg The String value representing an Option
     * @param iter The iterator over the flattened command line arguments.
     * @throws ParseException if <code>arg</code> does not represent an Option
     */
    @Override
    protected void processOption(String arg, @SuppressWarnings("rawtypes") ListIterator iter)
            throws ParseException {

        if (getOptions().hasOption(arg)) {
            super.processOption(arg, iter);
        }
    }
}