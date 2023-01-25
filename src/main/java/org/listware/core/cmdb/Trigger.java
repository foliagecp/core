/* Copyright 2022 Listware */

package org.listware.core.cmdb;

import java.util.HashMap;
import java.util.Map;

import org.listware.core.documents.LinkDocument;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.utils.exceptions.AlreadyTriggerException;
import org.listware.core.utils.exceptions.TriggerNotFoundException;
import org.listware.sdk.pbcmdb.Core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class Trigger {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Trigger.class);

	private static String triggersKey = "triggers";

	public static final String CREATE = "create";
	public static final String UPDATE = "update";
	public static final String DELETE = "delete";

	public Trigger() {
		// POJO
	}

	public Trigger(Core.Trigger trigger) {
		this();
		this.namespace = trigger.getFunctionType().getNamespace();
		this.type = trigger.getFunctionType().getType();
	}

	@JsonProperty("namespace")
	private String namespace;
	@JsonProperty("type")
	private String type;

	/**
	 *
	 * @return The namespace
	 */
	@JsonProperty("namespace")
	public String getNamespace() {
		return namespace;
	}

	/**
	 *
	 * @return The type
	 */
	@JsonProperty("type")
	public String getType() {
		return type;
	}

	public static Map<String, Trigger> getByType(ObjectDocument baseDocument, String type) throws Exception {
		Map<String, java.lang.Object> properties = baseDocument.getProperties();
		if (!properties.containsKey(triggersKey)) {
			throw new TriggerNotFoundException();
		}
		java.lang.Object object = properties.get(triggersKey);
		Map<String, Map<String, Trigger>> triggersMap = deserialize(object);

		if (!triggersMap.containsKey(type)) {
			throw new TriggerNotFoundException();
		}

		return triggersMap.get(type);
	}

	public static ObjectDocument add(ObjectDocument document, Core.Trigger trigger) throws Exception {
		String key = trigger.getFunctionType().getNamespace() + "/" + trigger.getFunctionType().getType();

		Map<String, java.lang.Object> properties = document.getProperties();

		Map<String, Map<String, Trigger>> triggersMap;
		if (properties.containsKey(triggersKey)) {
			java.lang.Object object = properties.get(triggersKey);
			triggersMap = deserialize(object);
		} else {
			triggersMap = new HashMap<String, Map<String, Trigger>>();
		}

		Map<String, Trigger> triggers;
		if (triggersMap.containsKey(trigger.getType())) {
			triggers = triggersMap.get(trigger.getType());
		} else {
			triggers = new HashMap<String, Trigger>();
		}

		if (triggers.containsKey(key)) {
			throw new AlreadyTriggerException();
		}

		triggers.put(key, new Trigger(trigger));

		triggersMap.put(trigger.getType(), triggers);

		properties.put(triggersKey, triggersMap);

		document.updateProperties(properties);

		return document;
	}

	public static LinkDocument add(LinkDocument document, Core.Trigger trigger) throws Exception {
		String key = trigger.getFunctionType().getNamespace() + "/" + trigger.getFunctionType().getType();

		Map<String, java.lang.Object> properties = document.getProperties();

		Map<String, Map<String, Trigger>> triggersMap;
		if (properties.containsKey(triggersKey)) {
			java.lang.Object object = properties.get(triggersKey);
			triggersMap = deserialize(object);
		} else {
			triggersMap = new HashMap<String, Map<String, Trigger>>();
		}

		Map<String, Trigger> triggers;
		if (triggersMap.containsKey(trigger.getType())) {
			triggers = triggersMap.get(trigger.getType());
		} else {
			triggers = new HashMap<String, Trigger>();
		}

		if (triggers.containsKey(key)) {
			throw new AlreadyTriggerException();
		}

		triggers.put(key, new Trigger(trigger));

		triggersMap.put(trigger.getType(), triggers);

		properties.put(triggersKey, triggersMap);

		document.updateProperties(properties);

		return document;
	}

	public static ObjectDocument delete(ObjectDocument document, Core.Trigger trigger) throws Exception {
		Map<String, java.lang.Object> properties = document.getProperties();
		java.lang.Object object = properties.get(triggersKey);

		// triggers map
		Map<String, Map<String, Trigger>> triggersMap = deserialize(object);

		// type map
		Map<String, Trigger> triggers = triggersMap.get(trigger.getType());

		String key = trigger.getFunctionType().getNamespace() + "/" + trigger.getFunctionType().getType();

		if (!triggers.containsKey(key)) {
			return document;
		}

		triggers.remove(key);

		triggersMap.put(trigger.getType(), triggers);

		properties.put(triggersKey, triggersMap);

		document.updateProperties(properties);
		return document;
	}

	public static LinkDocument delete(LinkDocument document, Core.Trigger trigger) throws Exception {
		Map<String, java.lang.Object> properties = document.getProperties();
		java.lang.Object object = properties.get(triggersKey);

		// triggers map
		Map<String, Map<String, Trigger>> triggersMap = deserialize(object);

		// type map
		Map<String, Trigger> triggers = triggersMap.get(trigger.getType());

		String key = trigger.getFunctionType().getNamespace() + "/" + trigger.getFunctionType().getType();

		if (!triggers.containsKey(key)) {
			return document;
		}

		triggers.remove(key);

		triggersMap.put(trigger.getType(), triggers);

		properties.put(triggersKey, triggersMap);

		document.updateProperties(properties);
		return document;
	}

	public static Map<String, Map<String, Trigger>> deserialize(Object from) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);

		TypeReference<Map<String, Map<String, Trigger>>> ref = new TypeReference<Map<String, Map<String, Trigger>>>() {
		};
		return mapper.convertValue(from, ref);
	}
}
