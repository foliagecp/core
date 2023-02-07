/*
 * Copyright 2022
 * Listware
 */

package org.listware.core.cmdb;

import org.listware.core.documents.LinkDocument;
import org.listware.core.utils.exceptions.PayloadNotFoundException;
import org.listware.core.utils.exceptions.UnknownIdException;
import org.listware.io.grpc.EdgeClient;
import org.listware.sdk.pbcmdb.Core;

import com.google.protobuf.ByteString;

public class LinkClient extends EdgeClient {

	public LinkDocument createDocument(String collection, ByteString payload) throws Exception {
		Core.Response resp = create(collection, payload);
		return LinkDocument.deserialize(resp.getPayload());
	}

	public LinkDocument readDocument(String id) throws Exception {
		Parser parser = new Parser(id);
		Core.Response resp = read(parser.getCollection(), parser.getKey());
		if (resp.getPayload().isEmpty()) {
			throw new PayloadNotFoundException();
		}
		return LinkDocument.deserialize(resp.getPayload());
	}

	public LinkDocument updateDocument(String id, ByteString payload) throws Exception {
		Parser parser = new Parser(id);
		Core.Response resp = update(parser.getCollection(), parser.getKey(), payload);
		if (resp.getPayload().isEmpty()) {
			throw new PayloadNotFoundException();
		}
		return LinkDocument.deserialize(resp.getPayload());
	}

	public LinkDocument replaceDocument(String id, ByteString payload) throws Exception {
		Parser parser = new Parser(id);
		Core.Response resp = replace(parser.getCollection(), parser.getKey(), payload);
		if (resp.getPayload().isEmpty()) {
			throw new PayloadNotFoundException();
		}
		return LinkDocument.deserialize(resp.getPayload());
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
