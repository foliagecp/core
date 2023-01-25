/* Copyright 2022 Listware */

package org.listware.core.documents;

import java.util.Map;

import org.listware.core.documents.entity.DocumentFields;
import org.listware.core.documents.entity.Name;
import org.listware.core.documents.entity.Type;
import org.listware.core.utils.exceptions.PayloadNotFoundException;

import com.arangodb.entity.From;
import com.arangodb.entity.To;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jackson.JsonLoader;
import com.google.protobuf.ByteString;

public class LinkDocument extends ObjectDocument {
	private static final long serialVersionUID = 6904923804449368783L;

	@From
	private String from;
	@To
	private String to;

	@Name
	private String name;

	@Type
	private String type;

	public LinkDocument() {
		super();
	}

	public LinkDocument(final String from, final String to) {
		super();
		this.from = from;
		this.to = to;
	}

	public LinkDocument(final String key, final String from, final String to) {
		super(key);
		this.from = from;
		this.to = to;
	}

	public LinkDocument(final String from, final String to, final String type, final String name) {
		this(from, to);
		this.name = name;
		this.type = type;
	}

	public LinkDocument(final Map<String, Object> properties) {
		super();
		replaceProperties(properties);
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(final String from) {
		this.from = from;
	}

	public String getTo() {
		return to;
	}

	public void setTo(final String to) {
		this.to = to;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	@Override
	public void replaceProperties(final Map<String, Object> properties) {
		final Object tmpFrom = properties.remove(DocumentFields.FROM);
		if (tmpFrom != null) {
			from = tmpFrom.toString();
		}
		final Object tmpTo = properties.remove(DocumentFields.TO);
		if (tmpTo != null) {
			to = tmpTo.toString();
		}
		final Object tmpName = properties.remove(DocumentFields.NAME);
		if (tmpName != null) {
			name = tmpName.toString();
		}
		final Object tmpType = properties.remove(DocumentFields.TYPE);
		if (tmpType != null) {
			type = tmpType.toString();
		}
		super.replaceProperties(properties);
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "BaseDocument [documentRevision=" + revision + ", documentHandle=" + id + ", documentKey=" + key
				+ ", from=" + from + ", to=" + to + ", properties=" + this.getProperties() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((from == null) ? 0 : from.hashCode());
		result = prime * result + ((to == null) ? 0 : to.hashCode());
		result = prime * result + ((to == null) ? 0 : name.hashCode());
		result = prime * result + ((to == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final LinkDocument other = (LinkDocument) obj;
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		if (type == null) {
			if (other.type != null) {
				return false;
			}
		} else if (!type.equals(other.type)) {
			return false;
		}
		if (from == null) {
			if (other.from != null) {
				return false;
			}
		} else if (!from.equals(other.from)) {
			return false;
		}
		if (to == null) {
			return other.to == null;
		} else
			return to.equals(other.to);
	}

	public static LinkDocument deserialize(ByteString payload) throws Exception {
		if (payload.isEmpty()) {
			throw new PayloadNotFoundException();
		}

		JsonNode jsonNode = JsonLoader.fromString(payload.toStringUtf8());
		ObjectMapper mapper = new ObjectMapper();

		TypeReference<Map<String, java.lang.Object>> ref = new TypeReference<Map<String, java.lang.Object>>() {
		};
		Map<String, java.lang.Object> values = mapper.convertValue(jsonNode, ref);
		return new LinkDocument(values);
	}
}
