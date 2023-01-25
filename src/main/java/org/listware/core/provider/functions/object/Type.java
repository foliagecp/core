/* Copyright 2022 Listware */

package org.listware.core.provider.functions.object;

import org.apache.flink.statefun.sdk.FunctionType;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.listware.core.FunctionContext;
import org.listware.core.cmdb.Trigger;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.utils.exceptions.UnknownIdException;
import org.listware.core.utils.exceptions.UnknownMethodException;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.Result;
import org.listware.sdk.pbcmdb.Core;

/**
 * Type CUD arangodb service for types
 **
 */
public class Type extends ObjectContext {
	private static final Logger LOG = LoggerFactory.getLogger(Type.class);

	public static final String TYPE = "types.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public Type() {
		super(TYPE, TYPE);
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
		ObjectDocument document = ObjectDocument.deserialize(message.getPayload());
		document.setKey(message.getName());

		document = cmdb.createType(document);
	}

	private void update(FunctionContext functionContext, Core.TypeMessage message) throws Exception {
		if (!functionContext.isType()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}
		ObjectDocument document = functionContext.getDocument();
		document.replaceProperties(message.getPayload());
		document.setId(functionContext.getFlinkContext().self().id());
		document = cmdb.updateType(functionContext.getFlinkContext(), document);
	}

	// TODO delete all objects with type
	private void delete(FunctionContext functionContext) throws Exception {
		if (!functionContext.isType()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}
		ObjectDocument document = functionContext.getDocument();
		document.setId(functionContext.getFlinkContext().self().id());

		cmdb.removeType(functionContext.getFlinkContext(), document);
	}

	private void createChild(FunctionContext functionContext, Core.TypeMessage message) throws Exception {
		if (!functionContext.isType()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}

		ObjectDocument document = ObjectDocument.deserialize(message.getPayload());
		ObjectDocument type = functionContext.getDocument();

		if (functionContext.getFlinkContext().caller() != null) {
			ObjectDocument parent = cmdb.readDocument(functionContext.getFlinkContext().caller().id());
			document = cmdb.createObject(functionContext.getFlinkContext(), type, parent, document, message.getName());
		} else {
			document = cmdb.createObject(functionContext.getFlinkContext(), type, document);
		}
	}

	private void createTrigger(FunctionContext functionContext, Core.TypeMessage message) throws Exception {
		if (!functionContext.isType()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}
		Core.Trigger trigger = Core.Trigger.parseFrom(message.getPayload());
		ObjectDocument document = Trigger.add(functionContext.getDocument(), trigger);

		document = cmdb.updateType(functionContext.getFlinkContext(), document);

		LOG.info("created type trigger " + document.getId());
	}

	private void deleteTrigger(FunctionContext functionContext, Core.TypeMessage message) throws Exception {
		if (!functionContext.isType()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}
		Core.Trigger trigger = Core.Trigger.parseFrom(message.getPayload());
		ObjectDocument document = Trigger.delete(functionContext.getDocument(), trigger);

		document = cmdb.updateType(functionContext.getFlinkContext(), document);

		LOG.info("deleted type trigger " + document.getId());
	}

	/**
	 * CreateObject create new object
	 **
	 * @param type        string
	 * @param name        string
	 * @param payload     ByteString
	 * @param replyEgress ReplyEgress (optional)
	 */
	public static Functions.FunctionContext CreateObject(String type, String name, ByteString payload,
			Result.ReplyResult replyResult) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(TYPE).build();

		Core.TypeMessage typeMessage = Core.TypeMessage.newBuilder().setMethod(Core.Method.CREATE_CHILD).setName(name)
				.setPayload(payload).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(type).setValue(typeMessage.toByteString());
		if (replyResult != null) {
			builder = builder.setReplyResult(replyResult);
		}
		return builder.build();
	}

}
