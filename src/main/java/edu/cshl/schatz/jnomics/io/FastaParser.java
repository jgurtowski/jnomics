package edu.cshl.schatz.jnomics.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

public class FastaParser implements Iterable<FastaParser.FastaRecord> {

    private BufferedReader reader;

    public FastaParser(InputStream in){
        reader = new BufferedReader(new InputStreamReader(in));
    }

    public Iterator<FastaRecord> iterator() {
        return new FastaIterator(reader);
    }

    public void close() throws IOException {
        reader.close();
    }

    public static class FastaIterator implements Iterator<FastaRecord> {

        private BufferedReader reader;
        private final FastaRecord record = new FastaRecord();
        private boolean nxt;

        private String prev_line = null;
        private String buff = "", line = null;
        
        public FastaIterator(BufferedReader reader) {
            this.reader = reader;
        }

        public boolean hasNext() {
            try {
                if(prev_line == null)
                    prev_line = reader.readLine();
                
                if(prev_line == null || !prev_line.startsWith(">")){
                    nxt = false;
                    return false;
                }
                record.setName(prev_line.substring(1));

                line = reader.readLine();
                buff = "";
                while(null != line && !line.startsWith(">")){
                    buff += line;
                    line = reader.readLine();
                }
                record.setSequence(buff);
                prev_line = line;
                nxt = true;
            } catch (Exception e) {
                nxt = false;
            }
            return nxt;
        }

        public FastaRecord next() {
            if (!nxt)
                return null;
            return record;
        }

        public void remove() {
        }

    }

    public static class FastaRecord {

        private String name;
        private String sequence;

        public FastaRecord() {
        }

        public FastaRecord(FastaRecord other) {
            this.name = other.getName();
            this.sequence = other.getSequence();
        }

        public void copy(FastaRecord other){
            this.name = other.getName();
            this.sequence = other.getSequence();
        }
        
        public void setName(String name) {
            this.name = name;
        }

        public void setSequence(String sequence) {
            this.sequence = sequence;
        }


        public String getName() {
            return name;
        }

        public String getSequence() {
            return sequence;
        }
    }
}