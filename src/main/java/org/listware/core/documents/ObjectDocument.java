/* Copyright 2022 Listware */

package org.listware.core.documents;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.listware.core.documents.entity.DocumentFields;
import org.listware.core.documents.entity.Meta;
import org.listware.core.utils.exceptions.PayloadNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.entity.Id;
import com.arangodb.entity.Key;
import com.arangodb.entity.Rev;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jackson.JsonLoader;
import com.google.protobuf.ByteString;

public class ObjectDocument implements Serializable {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(ObjectDocument.class);

	private static final long serialVersionUID = -1824742667228719116L;

	@Id
	protected String id;
	@Key
	protected String key;
	@Rev
	protected String revision;
	@Meta
	private MetaDocument meta;
	@JsonIgnore
	private Map<String, Object> properties;

	public ObjectDocument() {
		super();
		properties = new HashMap<>();
		meta = new MetaDocument();
	}

	public ObjectDocument(final String key) {
		this();
		this.key = key;
	}

	public ObjectDocument(final Map<String, Object> properties) {
		this();
		replaceProperties(properties);
	}

	public String getId() {
		return id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getKey() {
		return key;
	}

	public void setKey(final String key) {
		this.key = key;
	}

	public String getRevision() {
		return revision;
	}

	public void setRevision(final String revision) {
		this.revision = revision;
	}

	public MetaDocument getMeta() {
		return meta;
	}

	public void setMeta(MetaDocument meta) {
		this.meta = meta;
	}

	@JsonAnyGetter
	public Map<String, Object> getProperties() {
		return properties;
	}

	@JsonAnySetter
	public void setProperties(final Map<String, Object> properties) {
		this.properties = properties;
	}

	@SuppressWarnings("unchecked")
	public void replaceProperties(final Map<String, Object> properties) {
		final Object tmpId = properties.remove(DocumentFields.ID);
		if (tmpId != null) {
			id = tmpId.toString();
		}
		final Object tmpKey = properties.remove(DocumentFields.KEY);
		if (tmpKey != null) {
			key = tmpKey.toString();
		}
		final Object tmpRev = properties.remove(DocumentFields.REV);
		if (tmpRev != null) {
			revision = tmpRev.toString();
		}
		final Object tmpMeta = properties.remove(DocumentFields.META);
		if (tmpMeta != null) {
			meta = new MetaDocument((Map<String, Object>) tmpMeta);
		}
		this.properties = properties;
		meta.update();
	}

	public void replaceProperties(ByteString payload) throws Exception {
		if (payload.isEmpty()) {
			throw new PayloadNotFoundException();
		}

		JsonNode jsonNode = JsonLoader.fromString(payload.toStringUtf8());

		ObjectMapper mapper = new ObjectMapper();

		TypeReference<Map<String, Object>> ref = new TypeReference<Map<String, Object>>() {
		};

		Map<String, Object> values = mapper.convertValue(jsonNode, ref);
		replaceProperties(values);
	}

	public void addAttribute(final String key, final Object value) {
		properties.put(key, value);
		meta.update();
	}

	public void updateAttribute(final String key, final Object value) {
		if (properties.containsKey(key)) {
			properties.put(key, value);
			meta.update();
		}
	}

	public Object getAttribute(final String key) {
		return properties.get(key);
	}

	public void updateProperties(final Map<String, Object> properties) {
		properties.remove(DocumentFields.ID);
		properties.remove(DocumentFields.KEY);
		properties.remove(DocumentFields.REV);
		properties.remove(DocumentFields.META);
		this.properties = properties;
		meta.update();
	}

	@Override
	public String toString() {
		return "BaseDocument [documentRevision=" + revision + ", documentHandle=" + id + ", documentKey=" + key
				+ ", properties=" + properties + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((revision == null) ? 0 : revision.hashCode());
		result = prime * result + ((meta == null) ? 0 : meta.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final ObjectDocument other = (ObjectDocument) obj;
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id)) {
			return false;
		}
		if (key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!key.equals(other.key)) {
			return false;
		}
		if (properties == null) {
			if (other.properties != null) {
				return false;
			}
		} else if (!properties.equals(other.properties)) {
			return false;
		}
		if (meta == null) {
			if (other.meta != null) {
				return false;
			}
		} else if (!meta.equals(other.meta)) {
			return false;
		}
		if (revision == null) {
			return other.revision == null;
		} else
			return revision.equals(other.revision);
	}

	public ByteString serialize() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		byte[] values = mapper.writeValueAsBytes(this);
		return ByteString.copyFrom(values);
	}

	public static ObjectDocument deserialize(ByteString payload) throws Exception {
		if (payload.isEmpty()) {
			throw new PayloadNotFoundException();
		}

		JsonNode jsonNode = JsonLoader.fromString(payload.toStringUtf8());

		ObjectMapper mapper = new ObjectMapper();

		TypeReference<Map<String, Object>> ref = new TypeReference<Map<String, Object>>() {
		};

		Map<String, Object> values = mapper.convertValue(jsonNode, ref);

		return new ObjectDocument(values);
	}
}
