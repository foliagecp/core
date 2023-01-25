/*
 * Copyright 2022
 * Listware
 */

package org.listware.core.cmdb;

import org.listware.core.documents.ObjectDocument;
import org.listware.core.utils.exceptions.PayloadNotFoundException;
import org.listware.core.utils.exceptions.UnknownIdException;
import org.listware.io.grpc.VertexClient;
import org.listware.sdk.pbcmdb.Core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class ObjectClient extends VertexClient {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(ObjectClient.class);

	public ObjectDocument createDocument(String collection, ByteString payload) throws Exception {
		Core.Response resp = create(collection, payload);
		return ObjectDocument.deserialize(resp.getPayload());
	}

	public ObjectDocument readDocument(String id) throws Exception {
		Parser parser = new Parser(id);
		Core.Response resp = read(parser.getCollection(), parser.getKey());
		if (resp.getPayload().isEmpty()) {
			throw new PayloadNotFoundException();
		}
		return ObjectDocument.deserialize(resp.getPayload());
	}

	public ObjectDocument updateDocument(String id, ByteString payload) throws Exception {
		Parser parser = new Parser(id);
		Core.Response resp = update(parser.getCollection(), parser.getKey(), payload);
		if (resp.getPayload().isEmpty()) {
			throw new PayloadNotFoundException();
		}
		return ObjectDocument.deserialize(resp.getPayload());
	}

	public void removeDocument(String id) throws Exception {
		Parser parser = new Parser(id);
		remove(parser.getCollection(), parser.getKey());
	}

	class Parser {
		private String collection;
		private String key;

		public Parser(String id) throws Exception {
			String[] separated = id.split("\\/");
			if (separated.length < 2) {
				throw new UnknownIdException(id);
			}

			this.collection = separated[0];
			this.key = separated[1];
		}

		public String getCollection() {
			return collection;
		}

		public void setCollection(String collection) {
			this.collection = collection;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}

}
