/*
 *  Copyright 2023 NJWS Inc.
 *  Copyright 2022 Listware
 */

package org.listware.core.provider.functions.link;

import org.apache.flink.statefun.sdk.Context;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.core.FunctionContext;
import org.listware.core.cmdb.Trigger;
import org.listware.core.documents.LinkDocument;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.utils.exceptions.NoLinkException;
import org.listware.core.utils.exceptions.TriggerNotFoundException;
import org.listware.core.utils.exceptions.UnknownMethodException;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.util.JsonFormat;

public class LinkTrigger extends LinkContext {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(LinkTrigger.class);

	public static final String TYPE = "trigger.advanced.links.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public LinkTrigger() {
		super(TYPE, TYPE);
	}

	@Override
	public void invoke(FunctionContext functionContext) throws Exception {
		Core.LinkMessage message = Core.LinkMessage.parseFrom(functionContext.getFunctionContext().getValue());

		LinkDocument document = (LinkDocument) functionContext.getDocument();

		String fromType = cmdb.getTypeId(document.getFrom());
		String toType = cmdb.getTypeId(document.getTo());

		try {
			// type1 -> type2
			LinkDocument typesDocument = cmdb.readLinkDocumentByTo(fromType, toType);
			onTrigger(functionContext.getFlinkContext(), typesDocument, document.getFrom(), message.getMethod());
			onTrigger(functionContext.getFlinkContext(), typesDocument, document.getTo(), message.getMethod());
		} catch (NoLinkException ex) {
			LOG.debug(ex.getLocalizedMessage());
		} catch (TriggerNotFoundException ex) {
			LOG.debug(ex.getLocalizedMessage());
		}

		String functionTypeKey = "types/function";
		if (fromType.equals(functionTypeKey)) {
			try {
				executeFunctionTrigger(functionContext.getFlinkContext(), document, message.getMethod());
			} catch (Exception ex) {
				LOG.error(ex.getLocalizedMessage());
			}
		}
	}

	private void executeFunctionTrigger(Context context, LinkDocument document, Core.Method method) throws Exception {
		ObjectDocument functionDocument = cmdb.readDocument(document.getFrom());

		String functionTypeKey = "function_type";

		if (functionDocument.containsAttribute(functionTypeKey)) {

			java.lang.Object object = functionDocument.getAttribute(functionTypeKey);

			Functions.FunctionType.Builder builder = Functions.FunctionType.newBuilder();

			JsonFormat.parser().merge(object.toString(), builder);

			Functions.FunctionType functionType = builder.build();

			String namespace = functionType.getNamespace();
			String type = functionType.getType();

			String executeOnCreateKey = "execute_on_create";
			if (document.containsAttribute(executeOnCreateKey)) {
				java.lang.Object executeOnCreate = document.getAttribute(executeOnCreateKey);
				if (executeOnCreate instanceof Boolean) {
					Boolean flag = (Boolean) executeOnCreate;
					if (flag && method == Core.Method.CREATE) {
						executeTrigger(context, document.getTo(), namespace, type);
					}
				}
			}

			String executeOnUpdateKey = "execute_on_update";
			if (document.containsAttribute(executeOnUpdateKey)) {
				java.lang.Object executeOnUpdate = document.getAttribute(executeOnUpdateKey);
				if (executeOnUpdate instanceof Boolean) {
					Boolean flag = (Boolean) executeOnUpdate;
					if (flag && method == Core.Method.UPDATE) {
						executeTrigger(context, document.getTo(), namespace, type);
					}
				}
			}

		}
	}

	private void onTrigger(Context context, LinkDocument link, String id, Core.Method method) throws Exception {
		Map<String, Trigger> triggers = new HashMap<>();
		try {
			switch (method) {
			case CREATE:
				triggers = Trigger.getByType(link, Trigger.CREATE);
				break;

			case UPDATE:
				triggers = Trigger.getByType(link, Trigger.UPDATE);
				break;

			case DELETE:
				triggers = Trigger.getByType(link, Trigger.DELETE);
				break;

			default:
				throw new UnknownMethodException(method);
			}

			for (Trigger trigger : triggers.values()) {
				executeTrigger(context, id, trigger);
			}
		} catch (TriggerNotFoundException ignore) {
		}
	}

	private void executeTrigger(Context context, String id, Trigger trigger) throws Exception {
		Functions.FunctionContext pbFunctionContext = CreateFunctionContext(id, trigger.getNamespace(),
				trigger.getType());
		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);
		FunctionType functionType = new FunctionType(trigger.getNamespace(), trigger.getType());
		context.send(functionType, pbFunctionContext.getId(), typedValue);
	}

	private void executeTrigger(Context context, String id, String namespace, String type) throws Exception {
		Functions.FunctionContext pbFunctionContext = CreateFunctionContext(id, namespace, type);
		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);
		FunctionType functionType = new FunctionType(namespace, type);
		context.send(functionType, pbFunctionContext.getId(), typedValue);
	}

	/**
	 * Trigger trigger
	 **
	 * @param id     string
	 * @param method Method
	 */
	public static Functions.FunctionContext CreateTriggerFunctionContext(String id, Core.Method method)
			throws Exception {
		Core.LinkMessage message = Core.LinkMessage.newBuilder().setMethod(method).build();
		return CreateFunctionContext(id, Namespaces.INTERNAL, TYPE, message.toByteString());
	}

	public static void ExecuteTriggerFunction(Context context, String id, Core.Method method) throws Exception {
		Functions.FunctionContext pbFunctionContext = CreateTriggerFunctionContext(id, method);
		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);
		context.send(FUNCTION_TYPE, id, typedValue);
	}
}
