/* Copyright 2022 Listware */

package org.listware.core.documents;

import java.io.Serializable;
import java.util.Map;

import org.listware.core.documents.entity.Created;
import org.listware.core.documents.entity.DocumentFields;
import org.listware.core.documents.entity.Updated;

public class MetaDocument implements Serializable {
	private static final long serialVersionUID = 8363296886824978390L;

	@Created
	private Number created;

	@Updated
	private Number updated;

	public MetaDocument() {
		super();
		created = System.currentTimeMillis();
		updated = created;
	}

	public MetaDocument(final Map<String, Object> properties) {
		super();
		final Object tmpCreated = properties.remove(DocumentFields.CREATED);
		if (tmpCreated != null) {
			created = (Number) tmpCreated;
		}
		final Object tmpUpdated = properties.remove(DocumentFields.UPDATED);
		if (tmpUpdated != null) {
			updated = (Number) tmpUpdated;
		}
	}

	public Number getCreated() {
		return created;
	}

	public void setCreated(Number created) {
		this.created = created;
	}

	public Number getUpdated() {
		return updated;
	}

	public void setUpdated(Number updated) {
		this.updated = updated;
	}

	public void update() {
		updated = System.currentTimeMillis();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((created == null) ? 0 : created.hashCode());
		result = prime * result + ((updated == null) ? 0 : updated.hashCode());
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
		final MetaDocument other = (MetaDocument) obj;
		if (created == null) {
			if (other.created != null) {
				return false;
			}
		} else if (!created.equals(other.created)) {
			return false;
		}
		if (updated == null) {
			return other.updated == null;
		} else
			return updated.equals(other.updated);
	}
}
