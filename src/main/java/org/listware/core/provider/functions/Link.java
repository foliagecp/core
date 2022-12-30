/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import javax.annotation.Nullable;

import org.apache.flink.statefun.sdk.FunctionType;

import com.arangodb.ArangoGraph;
import com.arangodb.entity.EdgeUpdateEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.listware.core.FunctionContext;
import org.listware.core.documents.LinkDocument;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.utils.exceptions.AlreadyLinkException;
import org.listware.core.provider.utils.exceptions.NoLinkException;
import org.listware.core.provider.utils.exceptions.UnknownIdException;
import org.listware.core.provider.utils.exceptions.UnknownMethodException;

/**
 * Link CUD arangodb service for links
 **
 */
public class Link extends Arango {
	private static final Logger LOG = LoggerFactory.getLogger(Link.class);

	public static final String TYPE = "links.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public Link(ArangoGraph graph) {
		super(graph);
	}

	@Override
	public void invoke(FunctionContext functionContext) throws Exception {
		Core.LinkMessage message = Core.LinkMessage.parseFrom(functionContext.getFunctionContext().getValue());

		switch (message.getMethod()) {
		case CREATE:
			create(functionContext, message);
			break;

		case UPDATE:
			update(functionContext, message);
			break;

		case DELETE:
			delete(functionContext, message);
			break;

		default:
			throw new UnknownMethodException(message.getMethod());
		}
	}

	private void create(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		// do not create link from link
		if (functionContext.isLink()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}

		try {
			// or by from+name
			findByFromName(functionContext.getDocument().getId(), message.getName());
			throw new AlreadyLinkException(functionContext.getDocument().getId(), message.getName());
		} catch (NoLinkException ignored) {
		}

		ObjectDocument baseDocument = readContext(message.getTo());

		// FIXME _type?
		// types -> type == `Cmdb.TYPE_TYPE`
		// type -> object == type
		// object1 -> object2 == type of object2
		insertLink(functionContext.getDocument().getId(), baseDocument.getId(), message.getName(), message.getType(),
				message.getPayload());
	}

	private void update(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		LinkDocument document = null;

		if (!functionContext.isLink()) {
			document = findByFromName(functionContext.getDocument().getId(), message.getName());
		} else {
			document = (LinkDocument) functionContext.getDocument();
		}

		LinkDocument newDocument = LinkDocument.deserialize(message.getPayload());

		document.setProperties(newDocument.getProperties());

		document.updateMeta();

		EdgeUpdateEntity edgeUpdateEntity = links.replaceEdge(document.getKey(), document);
		LOG.info("updated link " + edgeUpdateEntity.getId());
	}

	private void delete(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		ObjectDocument prevLinkDocument = functionContext.getDocument();

		if (!functionContext.isLink()) {
			prevLinkDocument = findByFromName(functionContext.getDocument().getId(), message.getName());
		}

		links.deleteEdge(prevLinkDocument.getKey());
		LOG.info("deleted link " + prevLinkDocument.getId());
	}

	/**
	 * CreateLink create link 'from' -> 'to' with 'name'
	 **
	 * @param from     will be ('root', 'node', '17136214')
	 * @param to       will be ('root', 'node', '17136214')
	 * @param name     string
	 * @param callback FunctionContext (optional)
	 */
	public static Functions.FunctionContext CreateLink(String from, String to, String name,
			@Nullable Functions.FunctionContext callback) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(Link.TYPE).build();

		Core.LinkMessage linkMessage = Core.LinkMessage.newBuilder().setMethod(Core.Method.CREATE).setName(name)
				.setTo(to).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(from).setValue(linkMessage.toByteString());

		if (callback != null) {
			builder = builder.setCallback(callback);
		}
		return builder.build();
	}
}
