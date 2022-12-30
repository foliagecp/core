/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.statefun.sdk.Context;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoEdgeCollection;
import com.arangodb.ArangoGraph;
import com.arangodb.ArangoVertexCollection;
import com.arangodb.entity.EdgeEntity;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.listware.io.utils.QDSLClient;
import org.listware.io.utils.VertexClient;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.listware.core.FunctionContext;
import org.listware.core.documents.LinkDocument;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.utils.Cmdb.Collections;
import org.listware.core.provider.utils.Cmdb.Matcher;
import org.listware.core.provider.utils.Cmdb.SystemKeys;
import org.listware.core.provider.utils.exceptions.NoLinkException;

/**
 * Middleware for CUD functions
 **
 */
public class Arango extends Base {
	private static final Logger LOG = LoggerFactory.getLogger(Arango.class);

	private VertexClient vertexClient = new VertexClient();
	protected QDSLClient client = new QDSLClient();

	protected ArangoGraph graph;
	protected ArangoVertexCollection system, types, objects;
	protected ArangoEdgeCollection links;

	public Arango(ArangoGraph graph) {
		this.graph = graph;

		system = graph.vertexCollection(Collections.SYSTEM);
		types = graph.vertexCollection(Collections.TYPES);
		objects = graph.vertexCollection(Collections.OBJECTS);
		links = graph.edgeCollection(Collections.LINKS);
	}

	@Override
	public void invoke(Context context, Functions.FunctionContext pbFunctionContext) throws Exception {
		String id = context.self().id();
		ObjectDocument baseDocument = readContext(id);

		FunctionContext functionContext = new FunctionContext(context, baseDocument, pbFunctionContext);

		invoke(functionContext);

		functionContext.callback();
	}

	public void invoke(FunctionContext functionContext) throws Exception {

	}

	protected ObjectDocument readContext(String key) throws Exception {
		if (key.equals(SystemKeys.ROOT) || key.equals(SystemKeys.OBJECTS) || key.equals(SystemKeys.TYPES)) {

			Core.Response resp = vertexClient.read(key, Collections.SYSTEM);

			return ObjectDocument.deserialize(resp.getPayload());
		} else if (key.matches(Matcher.UUID_V4_STRING)) {
			Core.Response resp = vertexClient.read(key, Collections.OBJECTS);

			return ObjectDocument.deserialize(resp.getPayload());
		} else if (key.matches(Matcher.NUMERIC_STRING)) {
			Core.Response resp = vertexClient.read(key, Collections.LINKS);

			return LinkDocument.deserialize(resp.getPayload());
		} else {
			Core.Response resp = vertexClient.read(key, Collections.TYPES);

			return ObjectDocument.deserialize(resp.getPayload());
		}
	}

// *[?$._from == "%s" && $._name == "%s"?]

	protected LinkDocument findByFromName(String from, String name) throws NoLinkException {
//		QDSL.Options options = QDSL.Options.newBuilder().setObject(true).build();
//
//		String query = String.format("%s.objects", functionContext.Context().self().id());
//
//		QDSL.Elements elements = client.qdsl(query, options);
//		

		String query = "FOR t IN links FILTER t._name == @name && t._from == @from RETURN t";

		Map<String, java.lang.Object> bindVars = new HashMap<String, java.lang.Object>();
		bindVars.put("name", name);
		bindVars.put("from", from);
		ArangoCursor<LinkDocument> cursor = graph.db().query(query, bindVars, LinkDocument.class);

		if (cursor.hasNext()) {
			return cursor.next();
		}

		throw new NoLinkException(from, name);
	}

	private void insertLink(LinkDocument linkDocument) throws Exception {
		EdgeEntity edgeEntity = links.insertEdge(linkDocument);

		LOG.info("created " + edgeEntity.getId());

//		pbqdsl.Options options = pbqdsl.Options.newBuilder().setType(true).build();
//
//		String query = String.format("*[?$._to == '%s'?].objects", linkDocument.getFrom());
//
//		pbqdsl.Elements elements = client.qdsl(query, options);
//
//		if (elements.getElementsCount() > 0) {
//			String type = elements.getElements(0).getType();
//			LOG.info("insert: from " + type);
//		}
//
//		query = String.format("*[?$._to == '%s'?].objects", linkDocument.getTo());
//
//		elements = client.qdsl(query, options);
//
//		if (elements.getElementsCount() > 0) {
//			String type = elements.getElements(0).getType();
//			LOG.info("insert: to " + type);
//		}

	}

	protected void insertLink(String from, String to, String name, String type) throws Exception {
		LinkDocument linkDocument = new LinkDocument(from, to, name, type);
		insertLink(linkDocument);
	}

	protected void insertLink(String from, String to, String name, String type, ByteString payload) throws Exception {
		LinkDocument linkDocument = LinkDocument.deserialize(payload);
		linkDocument.setFrom(from);
		linkDocument.setTo(to);
		linkDocument.setName(name);
		linkDocument.setType(type);
		insertLink(linkDocument);
	}

	/**
	 * Exec function
	 **
	 * @param id        string
	 * @param namespace string
	 * @param type      string
	 * @param method    Method
	 */
	public static Functions.FunctionContext Exec(String id, String namespace, String type, Core.Method method) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(namespace).setType(type)
				.build();

		Core.TypeMessage typeMessage = Core.TypeMessage.newBuilder().setMethod(method).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(id).setValue(typeMessage.toByteString());
		return builder.build();
	}
}