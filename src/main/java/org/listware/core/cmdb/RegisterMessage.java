/* Copyright 2022 Listware */

package org.listware.core.cmdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.listware.core.provider.functions.Register;
import org.listware.sdk.pbcmdb.Core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterMessage {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Register.class);

	private List<byte[]> types = new ArrayList<>();
	private List<byte[]> objects = new ArrayList<>();;
	private List<byte[]> links = new ArrayList<>();;

	public RegisterMessage() {
		// POJO
	}

	public RegisterMessage(Core.RegisterMessage registerMessage) {
		registerMessage.getTypeMessagesList().forEach(msg -> types.add(msg.toByteArray()));
		registerMessage.getObjectMessagesList().forEach(msg -> objects.add(msg.toByteArray()));
		registerMessage.getLinkMessagesList().forEach(msg -> links.add(msg.toByteArray()));
	}

	public List<byte[]> getTypes() {
		return types;
	}

	public void setTypes(List<byte[]> types) {
		this.types = types;
	}

	public List<byte[]> getObjects() {
		return objects;
	}

	public void setObjects(List<byte[]> objects) {
		this.objects = objects;
	}

	public List<byte[]> getLinks() {
		return links;
	}

	public void setLinks(List<byte[]> links) {
		this.links = links;
	}

	public List<Core.RegisterTypeMessage> listTypes() throws Exception {
		List<Core.RegisterTypeMessage> registerMessages = new ArrayList<>();

		Iterator<byte[]> iterator = types.iterator();
		while (iterator.hasNext()) {
			byte[] data = iterator.next();
			Core.RegisterTypeMessage registerMessage = Core.RegisterTypeMessage.parseFrom(data);
			registerMessages.add(registerMessage);
			iterator.remove();
			if (!registerMessage.getAsync()) {
				return registerMessages;
			}
		}

		return registerMessages;
	}

	public List<Core.RegisterObjectMessage> listObjects() throws Exception {
		List<Core.RegisterObjectMessage> registerMessages = new ArrayList<>();

		Iterator<byte[]> iterator = objects.iterator();
		while (iterator.hasNext()) {
			byte[] data = iterator.next();
			Core.RegisterObjectMessage registerMessage = Core.RegisterObjectMessage.parseFrom(data);
			registerMessages.add(registerMessage);
			iterator.remove();
			if (!registerMessage.getAsync()) {
				return registerMessages;
			}
		}

		return registerMessages;
	}

	public List<Core.RegisterLinkMessage> listLinks() throws Exception {
		List<Core.RegisterLinkMessage> registerMessages = new ArrayList<>();

		Iterator<byte[]> iterator = links.iterator();
		while (iterator.hasNext()) {
			byte[] data = iterator.next();
			Core.RegisterLinkMessage registerMessage = Core.RegisterLinkMessage.parseFrom(data);
			registerMessages.add(registerMessage);
			iterator.remove();
			if (!registerMessage.getAsync()) {
				return registerMessages;
			}
		}

		return registerMessages;
	}

}
