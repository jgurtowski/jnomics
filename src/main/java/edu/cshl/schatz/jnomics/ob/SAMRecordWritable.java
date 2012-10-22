package edu.cshl.schatz.jnomics.ob;

import edu.cshl.schatz.jnomics.util.TextUtil;
import net.sf.samtools.*;
import net.sf.samtools.util.BufferedLineReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * User: james
 */
public class SAMRecordWritable implements WritableComparable,FastqStringProvider{

    private Text header = new Text();
    private Text readName = new Text();
    private IntWritable flags = new IntWritable();
    private Text referenceName = new Text();
    private IntWritable pos = new IntWritable();
    private IntWritable mapq = new IntWritable();
    private Text cigar = new Text();
    private Text mateReferenceName = new Text();
    private IntWritable mateStart = new IntWritable();
    private IntWritable insertSize = new IntWritable();
    private Text readString = new Text();
    private Text qualityString = new Text();
    private Text tags = new Text();
    private Map<String,SAMHeaderSequence> headerSequences;
    private final JnomicsTextTagCodec tagCodec = new JnomicsTextTagCodec();
    private final JnomicsSAMTextHeaderCodec headerCodec = new JnomicsSAMTextHeaderCodec();

    public SAMRecordWritable(){
    }

    public void set(SAMRecord samRecord){
        SAMFileHeader samHeader = samRecord.getHeader();
        String head = "@HD";
        if(null != samHeader.getTextHeader())
            head = samHeader.getTextHeader().trim();
        header.set(head);
        readName.set(samRecord.getReadName());
        flags.set(samRecord.getFlags());
        referenceName.set(samRecord.getReferenceName());
        pos.set(samRecord.getAlignmentStart());
        mapq.set(samRecord.getMappingQuality());
        cigar.set(samRecord.getCigarString());
        String mate = samRecord.getMateReferenceName();
        mateReferenceName.set(mate.compareTo(samRecord.getReferenceName()) == 0 ? "=" : mate);
        mateStart.set(samRecord.getMateAlignmentStart());
        insertSize.set(samRecord.getInferredInsertSize());
        readString.set(samRecord.getReadString());
        qualityString.set(samRecord.getBaseQualityString());

        List<String> tagStrings = new LinkedList<String>();

        for(SAMRecord.SAMTagAndValue tagAndValue: samRecord.getAttributes()){
            tagStrings.add(tagCodec.encode(tagAndValue.tag, tagAndValue.value));
        }
        tags.set(tagStrings.size() == 0 ? "" : TextUtil.join("\t",tagStrings));
    }

    @Override
    public int compareTo(Object o) {
        SAMRecordWritable of = (SAMRecordWritable)o;
        int diff = referenceName.compareTo(of.getReferenceName());
        if(diff != 0){
            diff = pos.compareTo(of.getAlignmentStart());
        }
        return diff;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        header.write(dataOutput);
        readName.write(dataOutput);
        flags.write(dataOutput);
        referenceName.write(dataOutput);
        pos.write(dataOutput);
        mapq.write(dataOutput);
        cigar.write(dataOutput);
        mateReferenceName.write(dataOutput);
        mateStart.write(dataOutput);
        insertSize.write(dataOutput);
        readString.write(dataOutput);
        qualityString.write(dataOutput);
        tags.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        header.readFields(dataInput);
        readName.readFields(dataInput);
        flags.readFields(dataInput);
        referenceName.readFields(dataInput);
        pos.readFields(dataInput);
        mapq.readFields(dataInput);
        cigar.readFields(dataInput);
        mateReferenceName.readFields(dataInput);
        mateStart.readFields(dataInput);
        insertSize.readFields(dataInput);
        readString.readFields(dataInput);
        qualityString.readFields(dataInput);
        tags.readFields(dataInput);
    }

    public Text getTextHeader(){
        return header;
    }
    
    public void setTags(String t){
        tags.set(t);
    }

    public void setTags(Text t){
        tags.set(t);
    }
    
    public Text getTags(){
        return tags;
    }
    
    public Text getReadName(){
        return readName;
    }
    
    public IntWritable getFlags(){
        return flags;
    }
    
    public Text getReferenceName(){
        return referenceName;
    }

    public IntWritable getAlignmentStart(){
        return pos;
    }
    
    public IntWritable getMappingQuality(){
        return mapq;
    }

    public Text getCigarString(){
        return cigar;
    }

    public Text getMateReferenceName(){
        return mateReferenceName;
    }
    public IntWritable getMateAlignmentStart(){
        return mateStart;
    }

    public IntWritable getInferredInsertSize(){
        return insertSize;
    }

    public Text getReadString(){
        return readString;
    }

    public Text getQualityString(){
        return qualityString;
    }

    public SAMRecord getSAMRecord(){
        SAMFileHeader samHeader = headerCodec.decode(new BufferedLineReader( new ByteArrayInputStream(header.getBytes())), null);
        SAMRecordFactory factory = new DefaultSAMRecordFactory();
        SAMRecord record = factory.createSAMRecord(samHeader);
        
        record.setHeader(samHeader);
        record.setReadName(readName.toString());
        record.setFlags(flags.get());
        record.setReferenceName(referenceName.toString());
        record.setAlignmentStart(pos.get());
        record.setMappingQuality(mapq.get());
        record.setCigarString(cigar.toString());
        record.setMateReferenceName(mateReferenceName.toString());
        record.setMateAlignmentStart(mateStart.get());
        record.setInferredInsertSize(insertSize.get());
        record.setReadString(readString.toString());
        record.setBaseQualityString(qualityString.toString());

        Map.Entry<String,Object> entry = null;
        for(String tagString : tags.toString().split("\t")){
            entry = tagCodec.decode(tagString);
            if(entry != null){
                if(entry.getValue() instanceof JnomicsTagValueAndUnsignedArrayFlag){
                    final JnomicsTagValueAndUnsignedArrayFlag valueAndFlag = (JnomicsTagValueAndUnsignedArrayFlag) entry.getValue();
                    if(valueAndFlag.isUnsignedArray){
                        record.setUnsignedArrayAttribute(entry.getKey(),valueAndFlag.value);
                    }else{
                        record.setAttribute(entry.getKey(), valueAndFlag.value);
                    }
                }else{
                    record.setAttribute(entry.getKey(), entry.getValue());
                }
            }
        }
        return record;
    }

    public String toString(){
        String []arr;
        if(tags.getLength() > 0){
            arr = new String[12];
            arr[11] = tags.toString();
        }else{
            arr = new String[11];
        }
        arr[0] = readName.toString();
        arr[1] = flags.toString();
        arr[2] = referenceName.toString();
        arr[3] = pos.toString();
        arr[4] = mapq.toString();
        arr[5] = cigar.toString();
        arr[6] = mateReferenceName.toString();
        arr[7] = mateStart.toString();
        arr[8] = insertSize.toString();
        arr[9] = readString.toString();
        arr[10] = qualityString.toString();
        return TextUtil.join("\t", arr);
    }

    public void parseHeader(){
        String header_str = header.toString();
        headerSequences = new HashMap<String, SAMHeaderSequence>();
        String name;
        int len;
        for(String line:header_str.split("\n")){
            if(line.startsWith("@SQ")){
                String []cols = line.split("\t");
                if(cols.length < 2 )
                    continue;
                if(!cols[1].startsWith("SN:") || !cols[2].startsWith("LN:"))
                    continue;
                name = cols[1].substring(cols[1].indexOf("SN:")+3);
                len = Integer.parseInt(cols[2].substring(cols[2].indexOf("LN:")+3));
                headerSequences.put(name, new SAMHeaderSequence(name,len));
            }
        }
    }

    public SAMHeaderSequence getHeaderSequence(String name){
        if(null == headerSequences)
            parseHeader();
        return headerSequences.get(name);
    }
    
    public String getFastqString(){
        return TextUtil.join(System.getProperty("line.separator"),new String[]{"@"+readName.toString(),
                readString.toString(),
                "+",
                qualityString.toString()});
    }
}
