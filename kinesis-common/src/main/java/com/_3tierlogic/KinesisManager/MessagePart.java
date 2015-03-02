package com._3tierlogic.KinesisManager;

import java.io.Serializable;
import java.util.UUID;



public class MessagePart implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public UUID uuid;
	public long part;
	public long of;
	public byte[] data;


	public MessagePart(UUID uuid, long part, long of, byte[] data) {
		this.uuid = uuid;
		this.part = part;
		this.of   = of;
		this.data = data;
	}

}
