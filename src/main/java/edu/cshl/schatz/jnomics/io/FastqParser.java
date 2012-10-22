package edu.cshl.schatz.jnomics.io;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.InputStream;
import java.util.Iterator;

public class FastqParser implements Iterable<FastqParser.FastqRecord> {

    private BufferedReader reader;

    public FastqParser(InputStream in){
        reader = new BufferedReader(new InputStreamReader(in));
    }

    public Iterator<FastqRecord> iterator() {
        return new FastqIterator(reader);
    }

    public void close() throws IOException {
        reader.close();
    }

    public static class FastqIterator implements Iterator<FastqRecord> {

        private BufferedReader reader;
        private final FastqRecord record = new FastqRecord();
        private boolean nxt;

        public FastqIterator(BufferedReader reader) {
            this.reader = reader;
        }

        public boolean hasNext() {
            try {
                record.setName(reader.readLine());
                record.setSequence(reader.readLine());
                record.setDescription(reader.readLine());
                record.setQuality(reader.readLine());
                if (record.getQuality() == null) {
                    nxt = false;
                }
                record.setName(record.getName().replaceFirst("@",""));
                record.setDescription(record.getDescription().replaceFirst("[+]",""));
                nxt = true;
            } catch (Exception e) {
                nxt = false;
            }
            return nxt;
        }

        public FastqRecord next() {
            if (!nxt)
                return null;
            return record;
        }

        public void remove() {
        }

    }

    public static class FastqRecord {

        private String name;
        private String sequence;
        private String description;
        private String quality;

        public FastqRecord() {
        }

        public FastqRecord(FastqRecord other) {
            this.name = other.getName();
            this.sequence = other.getSequence();
            this.description = other.getDescription();
            this.quality = other.getQuality();
        }

        public void copy(FastqRecord other){
            this.name = other.getName();
            this.sequence = other.getSequence();
            this.description = other.getDescription();
            this.quality = other.getQuality();
        }
        
        public void setName(String name) {
            this.name = name;
        }

        public void setSequence(String sequence) {
            this.sequence = sequence;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public void setQuality(String quality) {
            this.quality = quality;
        }

        public String getName() {
            return name;
        }

        public String getSequence() {
            return sequence;
        }

        public String getDescription() {
            return description;
        }

        public String getQuality() {
            return quality;
        }

    }
}