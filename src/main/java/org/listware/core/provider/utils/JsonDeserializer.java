/* Copyright 2022 Listware */

package org.listware.core.provider.utils;

import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


public class JsonDeserializer {
	public static Map<String, Map<String, Trigger>> triggers(Object from) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);

		TypeReference<Map<String, Map<String, Trigger>>> ref = new TypeReference<Map<String, Map<String, Trigger>>>() {
		};
		return mapper.convertValue(from, ref);
	}
}
