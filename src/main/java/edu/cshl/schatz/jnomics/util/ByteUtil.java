package edu.cshl.schatz.jnomics.util;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * User: james
 */
public class ByteUtil {

    /**
     * Compares the end of the db to the end of the query in reverse
     * Can be used to continuously check if the query appears in the
     * db as the db grows
     * @param db - db to be searched
     * @param query - query to be found
     * @return true if it is found, false if it is not
     */
    public static boolean reverseEndEqual(byte[] db, byte[] query){
        if(query.length > db.length)
            return false;

        int db_idx = db.length - 1;
        for(int j = query.length - 1; j>=0; j--){
            if(query[j] != db[db_idx])
                return false;
            db_idx--;
        }
        return true;
    }

    public static boolean reverseEndEqual(LinkedList<Byte> db, byte [] query){
        if(db.size() < query.length)
            return false;
        Iterator<Byte> it = db.descendingIterator();
        int i = query.length - 1;
        while(i >= 0){
            if(!it.hasNext())
                return false;
            if(it.next() != query[i])
                return false;
            i--;
        }
        return true;
    }

}
