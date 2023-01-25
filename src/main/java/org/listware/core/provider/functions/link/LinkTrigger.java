/*
 * Copyright 2022
 * Listware
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
import org.listware.core.utils.exceptions.UnknownMethodException;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.listware.sdk.pbcmdb.pbqdsl.QDSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

		String from = cmdb.getTypeId(document.getFrom());
		String to = cmdb.getTypeId(document.getTo());

		QDSL.Options options = QDSL.Options.newBuilder().setLink(true).build();

		String query = String.format("*[?@._id == '%s'?].*[?@._id == '%s'?].types", to, from);

		QDSL.Elements elements = cmdb.qdslClient.qdsl(query, options);

		for (QDSL.Element element : elements.getElementsList()) {
			LinkDocument link = LinkDocument.deserialize(element.getLink());

			Map<String, Trigger> triggers = new HashMap<>();

			switch (message.getMethod()) {
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
				throw new UnknownMethodException(message.getMethod());
			}

			for (Trigger trigger : triggers.values()) {
				execTrigger(functionContext.getFlinkContext(), document.getFrom(), trigger, message.getMethod());
				execTrigger(functionContext.getFlinkContext(), document.getTo(), trigger, message.getMethod());
			}

		}
	}

	private void execTrigger(Context context, String id, Trigger trigger, Core.Method method) {
		Functions.FunctionContext pbFunctionContext = Exec(id, trigger.getNamespace(), trigger.getType(), method);

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

		FunctionType functionType = new FunctionType(trigger.getNamespace(), trigger.getType());

		context.send(functionType, pbFunctionContext.getId(), typedValue);
	}

	/**
	 * Trigger trigger
	 **
	 * @param id     string
	 * @param method Method
	 */
	public static Functions.FunctionContext Trigger(String id, Core.Method method) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(TYPE).build();

		Core.LinkMessage message = Core.LinkMessage.newBuilder().setMethod(method).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(id).setValue(message.toByteString());
		return builder.build();
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
		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(id);
		return builder.build();
	}
}
