/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import com.arangodb.ArangoGraph;
import com.arangodb.entity.VertexEntity;
import com.arangodb.entity.VertexUpdateEntity;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.listware.core.FunctionContext;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.utils.Trigger;
import org.listware.core.provider.utils.Cmdb.LinkTypes;
import org.listware.core.provider.utils.Cmdb.SystemKeys;
import org.listware.core.provider.utils.exceptions.AlreadyLinkException;
import org.listware.core.provider.utils.exceptions.NoLinkException;
import org.listware.core.provider.utils.exceptions.UnknownIdException;
import org.listware.core.provider.utils.exceptions.UnknownMethodException;

/**
 * Type CUD arangodb service for types
 **
 */
public class Type extends Arango {
//	@Persisted
//    private final PersistedValue<String> TargetControllerData = PersistedValue.of("", String.class);

	private static final Logger LOG = LoggerFactory.getLogger(Type.class);

	public static final String TYPE = "types.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public Type(ArangoGraph graph) {
		super(graph);
	}

	@Override
	public void invoke(FunctionContext functionContext) throws Exception {
		// TODO only create from 'types.root' or work with 'type'
		if (!functionContext.isType() && !functionContext.isTypes()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}

		Core.TypeMessage message = Core.TypeMessage.parseFrom(functionContext.getFunctionContext().getValue());

		switch (message.getMethod()) {
		case CREATE:
			create(functionContext, message);
			break;

		case CREATE_CHILD:
			createChild(functionContext, message);
			break;

		case UPDATE:
			update(functionContext, message);
			break;

		case DELETE:
			delete(functionContext);
			break;

		case CREATE_TRIGGER:
			createTrigger(functionContext, message);
			break;

		case DELETE_TRIGGER:
			deleteTrigger(functionContext, message);
			break;

		default:
			throw new UnknownMethodException(message.getMethod());
		}
	}

	private void create(FunctionContext functionContext, Core.TypeMessage message) throws Exception {
		if (!functionContext.isTypes()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}

		ObjectDocument baseDocument = ObjectDocument.deserialize(message.getPayload());
		baseDocument.setKey(message.getName());

		VertexEntity vertexEntity = types.insertVertex(baseDocument);

		LOG.info("created type " + vertexEntity.getId());

		// TODO link from types -> type
		insertLink(functionContext.getDocument().getId(), vertexEntity.getId(), message.getName(), LinkTypes.TYPE);
	}

	private void update(FunctionContext functionContext, Core.TypeMessage message) throws Exception {
		ObjectDocument document = functionContext.getDocument();

		ObjectDocument newDocument = ObjectDocument.deserialize(message.getPayload());

		document.setProperties(newDocument.getProperties());

		document.updateMeta();

		VertexUpdateEntity vertexUpdateDocument = types.replaceVertex(functionContext.getFlinkContext().self().id(),
				document);

		LOG.info("updated type " + vertexUpdateDocument.getId());
	}

	// TODO delete all objects with type
	private void delete(FunctionContext functionContext) throws Exception {
		types.deleteVertex(functionContext.getFlinkContext().self().id());
		LOG.info("deleted type " + functionContext.getFlinkContext().self().id());
	}

	private void createChild(FunctionContext functionContext, Core.TypeMessage message) throws Exception {
		ObjectDocument callerBaseDocument = null;

		if (functionContext.getFlinkContext().caller() != null) {
			try {
				callerBaseDocument = readContext(functionContext.getFlinkContext().caller().id());
				try {
					// do not duplicate link with name
					findByFromName(callerBaseDocument.getId(), message.getName());

					throw new AlreadyLinkException(callerBaseDocument.getId(), message.getName());
				} catch (NoLinkException ignored) {
				}
			} catch (IllegalArgumentException ignored) {
			}
		}
		ObjectDocument baseDocument = ObjectDocument.deserialize(message.getPayload());

		VertexEntity vertexEntity = objects.insertVertex(baseDocument);

		// Link type -> object ($uuid)
		insertLink(functionContext.getDocument().getId(), vertexEntity.getId(), vertexEntity.getKey(),
				functionContext.getDocument().getKey());

		// trigger!!!
		Functions.FunctionContext pbFunctionContext = ObjectTrigger.Trigger(vertexEntity.getKey(), Core.Method.CREATE);

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

		functionContext.getFlinkContext().send(ObjectTrigger.FUNCTION_TYPE, vertexEntity.getKey(), typedValue);

		// Link objects -> object ($uuid)
		ObjectDocument objects = system.getVertex(SystemKeys.OBJECTS, ObjectDocument.class);
		insertLink(objects.getId(), vertexEntity.getId(), vertexEntity.getKey(),
				functionContext.getDocument().getKey());

		// if caller, link caller -> object (name)
		if (callerBaseDocument != null) {
			insertLink(callerBaseDocument.getId(), vertexEntity.getId(), message.getName(),
					functionContext.getDocument().getKey());
		}
	}

	private void createTrigger(FunctionContext functionContext, Core.TypeMessage message) throws Exception {
		Core.Trigger trigger = Core.Trigger.parseFrom(message.getPayload());

		ObjectDocument document = Trigger.add(functionContext.getDocument(), trigger);

		document.updateMeta();

		VertexUpdateEntity vertexUpdateDocument = types.replaceVertex(functionContext.getFlinkContext().self().id(),
				document);
		LOG.info("created trigger " + vertexUpdateDocument.getId());
	}

	private void deleteTrigger(FunctionContext functionContext, Core.TypeMessage message) throws Exception {
		Core.Trigger trigger = Core.Trigger.parseFrom(message.getPayload());

		ObjectDocument document = Trigger.delete(functionContext.getDocument(), trigger);

		document.updateMeta();

		VertexUpdateEntity vertexUpdateDocument = types.replaceVertex(functionContext.getFlinkContext().self().id(),
				document);
		LOG.info("deleted trigger " + vertexUpdateDocument.getId());
	}

	/**
	 * CreateObject create new object
	 **
	 * @param type     string
	 * @param name     string
	 * @param payload  ByteString
	 * @param callback FunctionContext (optional)
	 */
	public static Functions.FunctionContext CreateObject(String type, String name, ByteString payload,
			Functions.FunctionContext callback) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(TYPE).build();

		Core.TypeMessage typeMessage = Core.TypeMessage.newBuilder().setMethod(Core.Method.CREATE_CHILD).setName(name)
				.setPayload(payload).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(type).setValue(typeMessage.toByteString());
		if (callback != null) {
			builder = builder.setCallback(callback);
		}
		return builder.build();
	}

}
