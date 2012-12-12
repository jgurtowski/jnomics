package edu.cshl.schatz.jnomics.ob;

/**
 * User: james
 */

import org.msgpack.annotation.Message;
import org.msgpack.annotation.Optional;

/**
 * MessagePack Sequencing Read
 */

@Message
public class SequencingRead {
    public String name;
    public String sequence;

    @Optional
    public String description;
    @Optional
    public String quality;
}
