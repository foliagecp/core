/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import com.arangodb.ArangoGraph;
import com.arangodb.entity.VertexUpdateEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.listware.core.FunctionContext;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.utils.exceptions.UnknownIdException;
import org.listware.core.provider.utils.exceptions.UnknownMethodException;

/**
 * Object CUD arangodb service for objects
 **
 */
public class Object extends Arango {
	private static final Logger LOG = LoggerFactory.getLogger(Object.class);

	public static final String TYPE = "objects.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public Object(ArangoGraph graph) {
		super(graph);
	}

	@Override
	public void invoke(FunctionContext functionContext) throws Exception {
		Core.ObjectMessage message = Core.ObjectMessage.parseFrom(functionContext.getFunctionContext().getValue());

		switch (message.getMethod()) {
		case CREATE_CHILD:
			createChild(functionContext, message);
			break;

		case UPDATE:
			update(functionContext, message);
			break;

		case DELETE:
			delete(functionContext);
			break;

		default:
			throw new UnknownMethodException(message.getMethod());
		}
	}

	private void createChild(FunctionContext functionContext, Core.ObjectMessage message) throws Exception {
		Functions.FunctionContext pbFunctionContext = Type.CreateObject(message.getType(), message.getName(),
				message.getPayload(), functionContext.getCallback());

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

		functionContext.getFlinkContext().send(Type.FUNCTION_TYPE, message.getType(), typedValue);
	}

	private void update(FunctionContext functionContext, Core.ObjectMessage message) throws Exception {
		if (!functionContext.isObject()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}

		ObjectDocument document = functionContext.getDocument();

		ObjectDocument newDocument = ObjectDocument.deserialize(message.getPayload());

		document.setProperties(newDocument.getProperties());

		document.updateMeta();

		VertexUpdateEntity vertexUpdateDocument = types.replaceVertex(functionContext.getFlinkContext().self().id(),
				document);

		LOG.info("updated object " + vertexUpdateDocument.getId());
	}

	private void delete(FunctionContext functionContext) throws Exception {
		if (!functionContext.isObject()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}

		objects.deleteVertex(functionContext.getFlinkContext().self().id());
		LOG.info("deleted object " + functionContext.getFlinkContext().self().id());
	}
}
