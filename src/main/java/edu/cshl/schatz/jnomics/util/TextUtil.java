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

package edu.cshl.schatz.jnomics.util;

import java.util.Arrays;

import org.apache.hadoop.io.Text;

/**
 * A utility that provides various methods for {@link Text} comparison and
 * manipulation, particularly efficient analogs to those provided for
 * {@link String}.
 * 
 * @author Matthew A. Titmus
 */
public class TextUtil {
    /**
     * Determines whether the string represented by the {@link Text} instance
     * <code>query</code> begins with the string represented by the {@link Text}
     * instance <code>prefix</code>.
     * 
     * @param query
     * @param prefix
     * @return <code>True</code> if <code>query</code> starts with
     *         <code>prefix</code>; <code>false</code> otherwise.
     */
    public static boolean startsWith(Text query, Text prefix) {
        if (prefix.getLength() < query.getLength()) {
            byte[] testBytes = new byte[query.getBytes().length];
            int pLen = prefix.getLength();

            System.arraycopy(prefix.getBytes(), 0, testBytes, 0, pLen);
            System.arraycopy(query.getBytes(), pLen, testBytes, pLen, query.getLength() - pLen);

            boolean result = Arrays.equals(query.getBytes(), testBytes);

            return result;
        }

        return false;
    }

    /**
     * Joins an array of strings with delim
     * @param delim delimiter (set to null for no delimiter)
     * @param arr array of strings
     * @return string of joined array elements
     */

    public static String join(String delim, Iterable<String> arr){
        StringBuilder builder = new StringBuilder();
        boolean first =  true;
        for(String s: arr){
            if(first){
                first = false;
            }else{
                builder.append(delim);
            }
            builder.append(s);
        }
        return builder.toString();
    }

    public static String join(String delim, String []arr){
        StringBuilder builder = new StringBuilder();
        boolean first =  true;
        for(String s: arr){
            if(first){
                first = false;
            }else{
                builder.append(delim);
            }
            builder.append(s);
        }
        return builder.toString();
    }

}
