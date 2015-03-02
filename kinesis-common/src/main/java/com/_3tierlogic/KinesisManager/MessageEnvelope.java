package com._3tierlogic.KinesisManager;

import java.io.Serializable;
import java.util.UUID;


/** 
 * This is defined in Java because serialization in Scala is only for masochists.
 * Scala has so much extra wrapping paper, Java serialization is impractical.
 * There is supposed to be Scala 'pickling' that does the same, but you need
 * a PhD. in Scala Pickling before you can use it.
 * 
 * @author Eric Kolotyluk
 *
 */
public class MessageEnvelope implements Serializable {
	
	private static final long serialVersionUID = 1L;
		
	public byte[] data;
	public String type;
	public UUID uuid;
	public long time;
	public long nano;

	public MessageEnvelope(byte[] data, String type, UUID uuid, long time, long nano) {
		this.data = data;
		this.type = type;
		this.uuid = uuid;
		this.time = time;
		this.nano = nano;
	}

}
