package com._3tierlogic.KinesisManager;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;


/** 
 * This is defined in Java because serialization in Scala is only for masochists.
 * Scala has so much extra wrapping paper, Java serialization is impractical.
 * There is supposed to be Scala 'pickling' that does the same, but you need
 * a PhD. in Scala Pickling before you can use it.
 * 
 * @author Eric Kolotyluk
 *
 * @see <a href="http://www.iana.org/assignments/media-types/media-types.xhtml">Mime Media Types</a>
 */
public class MessageEnvelope implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	// Serialized members
	byte[] data;
	String type;
	String mime;
	UUID uuid;
	long time;
	long nano;
	
	// Unserialized members
	transient ArrayList<String> warnings;
	transient SimpleDateFormat simpleDateFormat;

	public MessageEnvelope(byte[] data, String type, String mime, UUID uuid, long time, long nano) {
		putData(data);
		putType(type);
		putMime(mime);
		putUUID(uuid);
		putTime(time);
		putNano(nano);
	}
	
	public MessageEnvelope(byte[] data, String type, String mime, String uuid, String time, String nano) {
		putData(data);
		putType(type);
		putMime(mime);
		putUUID(uuid);
		putTime(time);
		putNano(nano);
	}
	
	public boolean isEmpty() {
		return data == null || data.length == 0;
	}
	
	// Getters
	
	public byte[] getData() {return data;}

	public String getType() {return type;}

	/**
	 * @return <a href="http://www.iana.org/assignments/media-types/media-types.xhtml">Mime Media Type</a>
	 *         of the message
	 */
	public String getMine() {return mime;}

	public UUID   getUUID() {return uuid;}
	public String getUUIDAsString() {return uuid.toString();}

	public long   getTime() {return time;}
	public String getTimeAsString() {
		if (simpleDateFormat == null) simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		return simpleDateFormat.format(time);
	}

	public long   getNano() {return nano;}
	public String getNanoAsString() {return Long.toString(nano);}
	
	public List<String> getWarnings() {
		if (warnings == null) return Collections.emptyList();
		return warnings;
	}
	
	// putters

	public void putData(byte[] data) {
		if (data == null) addWarning("data is null"); else this.data = data;
	}
	
	public void putType(String type) {
		if (type == null) {
			this.type = "unknown";
			addWarning("type is null; default set to '" + this.type + "'");
		}
		else this.type = type;
	}
	
	public void putMime(String mime) {
		if (mime == null) {
			this.mime = "example/unknown";
			addWarning("mime is null; default set to '" + this.mime);
		}
		else this.mime = mime;
	}
	
	public void putUUID(UUID uuid) {
		if (uuid == null) {
			this.uuid = UUID.randomUUID();
			addWarning("uuid is null; default set to " + this.uuid);
		}
		else this.uuid = uuid;
	}
	
	public void putUUID(String uuid) {
		try {
			this.uuid = UUID.fromString(uuid);
		}
		catch (IllegalArgumentException e) {
			this.uuid = UUID.randomUUID();
			addWarning("uuid '" + uuid + "' is invalid; default set to " + this.uuid);
		}
	}
	
	public void putTime(long time) {
		if (time <= 0) {
			this.time = System.currentTimeMillis();
			addWarning("time is " + time + "; default set to " + this.time);
		}
		else this.time = time;
	}
	
	public void putTime(String time) {
		if (simpleDateFormat == null) simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");

		try {
			this.time = simpleDateFormat.parse(time).getTime();
		}
		catch (ParseException e) {
			this.time = System.currentTimeMillis();
			addWarning("time '" + time + "' is invalid; default set to " + simpleDateFormat.format(this.time));
		}
	}
	
	public void putNano(long nano) {
		if (nano <= 0) {
			this.time = System.nanoTime();
			addWarning("nano is " + nano + "; default set to " + this.nano);
		}
		else {
			this.nano = nano;
		}
	}
	
	public void putNano(String nano) {
		try {
			this.nano = Long.parseLong(nano);
		}
		catch (NumberFormatException e) {
			this.nano = System.nanoTime();
			addWarning("nano '" + nano + "' is invalid; default set to " + this.nano);
		}
	}
	
	private void addWarning(String warning) {
		if (warnings == null) warnings = new ArrayList<String>();
		warnings.add(warning);
	}
}
