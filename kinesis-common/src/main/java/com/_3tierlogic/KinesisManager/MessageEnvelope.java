package com._3tierlogic.KinesisManager;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;


/**
 * <strong>Generic Message Envelope used to wrap all consumed messages</strong>
 * <p>
 * This is generic message format used by both the producer and consumer data streams.
 * It is a high-level format akin to files on a file system, whereas raw Kinesis
 * messages and shards are akin to sectors, tracks, and cylinders on a disk drive.
 * <p>
 * This is defined in Java because serialization in Scala is only for masochists.
 * Scala has so much extra wrapping paper, Java serialization is impractical.
 * There is supposed to be Scala 'pickling' that does the same, but you need
 * a PhD. in Scala Pickling before you can use it.
 * 
 * @author Eric Kolotyluk
 *
 * @see <a href="http://docs.oracle.com/javase/8/docs/api/java/beans/package-summary.html">Java Beans</a>
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

	/**
	 * <strong>Construct a MessageEnvelope</strong>
	 * <p>
	 *   Construct a MessageEnvelope used to serialize each message in the producer data stream.
	 * <p>
	 * 
	 * @param data {@link #setData(byte[])}
	 * @param type {@link #setType(String)}
	 * @param mime {@link #setMime(String)}
	 * @param uuid {@link #setUUID(UUID)}
	 * @param time {@link #setTime(long)}
	 * @param nano {@link #setNano(long)}
	 */
	public MessageEnvelope(byte[] data, String type, String mime, UUID uuid, long time, long nano) {
		setData(data);
		setType(type);
		setMime(mime);
		setUUID(uuid);
		setTime(time);
		setNano(nano);
	}

	/**
	 * <strong>Construct a MessageEnvelope</strong>
	 * <p>
	 *   Construct a MessageEnvelope used to serialize each message in the producer data stream.
	 * <p>
	 *   This constructor is intended to be used when processing HTTP POST/PUT requests with string
	 *   parameters.
	 * 
	 * @param data {@link #setData(byte[])}
	 * @param type {@link #setType(String)}
	 * @param mime {@link #setMime(String)}
	 * @param uuid {@link #setUUID(String)}
	 * @param time {@link #setTime(String)}
	 * @param nano {@link #setNano(String)}
	 */
	public MessageEnvelope(byte[] data, String type, String mime, String uuid, String time, String nano) {
		setData(data);
		setType(type);
		setMime(mime);
		setUUID(uuid);
		setTime(time);
		setNano(nano);
	}
	
	/**
	 * <strong>Construct a MessageEnvelope with default properties</strong>
	 * <p>
	 *   Construct a MessageEnvelope used to serialize each message in the producer data stream.
	 * <p>
	 * 
	 * @param data {@link #setData(byte[])}
	 */
	public MessageEnvelope(byte[] data) {
		setData(data);
		setType("unknown");
		setMime("");
		setUUID(UUID.randomUUID());
		setTime(System.currentTimeMillis());
		setNano(System.nanoTime());
	}

	/**
	 * <strong>Determine if MessageEnvelope has no data</strong>
	 * <p>
	 * Messages with no data are of no value, and are discarded before entering the stream.
	 * <p>
	 * @return
	 */
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

	/**
	 * <strong>Get possible warnings from property setting.</strong>
	 * <p>
	 * This is an error free class, in that you cannot construct an object with errors in it.
	 * If you try to set a property with an bad value, a warning will be generated, and a correct
	 * default value will be used.
	 * <p>
	 * @return a list of warnings, or the empty list.
	 */
	public List<String> getWarnings() {
		if (warnings == null) return Collections.emptyList();
		return warnings;
	}
	
	// putters

	/**
	 * <p><strong>Set raw data into the message envelope</strong></p>
	 * 
	 * From {@link java.util.ArrayList} the maximum size of an array is:
	 * <pre>
	 * private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
	 * </pre>
	 * @param data - 2,147,483,639 or fewer bytes of raw data.
	 * 
	 * @see java.lang.Integer.MAX_VALUE
	 * 
	 * TODO - The data size needs to be smaller because of other overheads...
	 */
	public void setData(byte[] data) {
		if (data == null) addWarning("data is null"); else this.data = data;
	}
	
	/**
	 * <p><strong>Put the application specific message type</strong></p>
	 * 
	 * This can be anything you want to distinguish one type of message from another.
	 * If this is null or the empty string, the default value "unknown" is used.
	 * 
	 * @param type arbitrary string
	 */
	public void setType(String type) {
		if (type == null || type == "") {
			this.type = "unknown";
			addWarning("type is null; default set to '" + this.type + "'");
		}
		else this.type = type;
	}
	
	/**
	 * <strong>Set the Mine Media Type</strong>
	 * <p>
	 * This is not currently used for anything, but may be used in the future for internal
	 * encoding/decoding such as compression/decompression. If this is null, the default
	 * value is the emptry string ""
	 * <p>
	 * @param mime media type
	 * 
	 * @see <a href="http://www.iana.org/assignments/media-types/media-types.xhtml">Mime Media Types</a>
	 */
	public void setMime(String mime) {
		if (mime == null) {
			this.mime = "example/unknown";
			addWarning("mime is null; default set to '" + this.mime);
		}
		else this.mime = mime;
	}
	
	/**
	 * <strong>Set Universally Unique Identifier</strong>
	 * <p>
	 * An ID to uniquely identify this message in the message stream. If this is null,
	 * the default value is a randomly generated UUID.
	 * <p>
	 * @param uuid
	 */
	public void setUUID(UUID uuid) {
		if (uuid == null) {
			this.uuid = UUID.randomUUID();
			addWarning("uuid is null; default set to " + this.uuid);
		}
		else this.uuid = uuid;
	}
	
	public void setUUID(String uuid) {
		try {
			this.uuid = UUID.fromString(uuid);
		}
		catch (IllegalArgumentException e) {
			this.uuid = UUID.randomUUID();
			addWarning("uuid '" + uuid + "' is invalid; default set to " + this.uuid);
		}
	}
	
	/**
	 * <strong>Set timestamp on the message.</strong>
	 * <p>
	 * Sequence order of messages in the stream are not maintained. The time and nano
	 * properties are the only way to impose an ordering to message that can be recovered
	 * later.
	 * <p>
	 * If time is <= 0 the default value of System.currentTimeMillis() is used instead.
	 * <p>
	 * @param time
	 */
	public void setTime(long time) {
		if (time <= 0) {
			this.time = System.currentTimeMillis();
			addWarning("time is " + time + "; default set to " + this.time);
		}
		else this.time = time;
	}
	
	/**
	 * <strong>Set timestamp from a datetime string.</strong>
	 * <p>
	 * Parse the datetime string for a valid time value and set the value as in {@link #setTime(long)}
	 * <p>
	 * If time does not parse to a valid datetype according to the pattern, the default
	 * value of System.currentTimeMillis() is used instead.
	 * @param time in the format yyyy-MM-dd'T'hh:mm:ss.SSS'Z'
	 */
	public void setTime(String time) {
		if (simpleDateFormat == null) simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");

		try {
			this.time = simpleDateFormat.parse(time).getTime();
		}
		catch (ParseException e) {
			this.time = System.currentTimeMillis();
			addWarning("time '" + time + "' is invalid; default set to " + simpleDateFormat.format(this.time));
		}
	}
	
	/**
	 * <strong>Set higher resolution timestamp</strong>
	 * <p>
	 * At high ingestion rates, it is easy to generate messages within the same millisecond
	 * resolution of {@link java.lang.System#currentTimeMillis()} so this can add some extra
	 * precision for sequencing messages.
	 * <p>
	 * @param nano
	 */
	public void setNano(long nano) {
		if (nano <= 0) {
			this.time = System.nanoTime();
			addWarning("nano is " + nano + "; default set to " + this.nano);
		}
		else {
			this.nano = nano;
		}
	}
	
	public void setNano(String nano) {
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
