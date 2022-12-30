/* Copyright 2022 Listware */

package org.listware.core.provider.utils;

import java.util.HashMap;
import java.util.Map;

import org.listware.sdk.pbcmdb.Core;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.utils.exceptions.AlreadyTriggerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Trigger {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Trigger.class);

	private static String triggersKey = "triggers";

	public Trigger(Core.Trigger trigger) {
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
		java.lang.Object object = properties.get(triggersKey);
		Map<String, Map<String, Trigger>> triggersMap = JsonDeserializer.triggers(object);
		return triggersMap.get(type);
	}

	public static ObjectDocument add(ObjectDocument baseDocument, Core.Trigger trigger) throws Exception {
		String key = trigger.getFunctionType().getNamespace() + "/" + trigger.getFunctionType().getType();

		Map<String, java.lang.Object> properties = baseDocument.getProperties();

		Map<String, Map<String, Trigger>> triggersMap;
		if (properties.containsKey(triggersKey)) {
			java.lang.Object object = properties.get(triggersKey);
			triggersMap = JsonDeserializer.triggers(object);
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

		baseDocument.setProperties(properties);
		return baseDocument;
	}

	public static ObjectDocument delete(ObjectDocument baseDocument, Core.Trigger trigger) throws Exception {
		Map<String, java.lang.Object> properties = baseDocument.getProperties();
		java.lang.Object object = properties.get(triggersKey);

		// triggers map
		Map<String, Map<String, Trigger>> triggersMap = JsonDeserializer.triggers(object);

		// type map
		Map<String, Trigger> triggers = triggersMap.get(trigger.getType());

		String key = trigger.getFunctionType().getNamespace() + "/" + trigger.getFunctionType().getType();

		if (!triggers.containsKey(key)) {
			return baseDocument;
		}

		triggers.remove(key);

		triggersMap.put(trigger.getType(), triggers);

		properties.put(triggersKey, triggersMap);

		baseDocument.setProperties(properties);
		return baseDocument;
	}
}
