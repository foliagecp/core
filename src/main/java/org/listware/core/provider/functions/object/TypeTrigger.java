/*
 *  Copyright 2023 NJWS Inc.
 *  Copyright 2022 Listware
 */

package org.listware.core.provider.functions.object;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.core.FunctionContext;
import org.listware.core.cmdb.Trigger;
import org.listware.core.utils.exceptions.TriggerNotFoundException;
import org.listware.core.utils.exceptions.UnknownMethodException;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypeTrigger extends ObjectContext {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(TypeTrigger.class);

	private static final String TYPE = "trigger.types.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public TypeTrigger() {
		super(TYPE, TYPE);
	}

	@Override
	public void invoke(FunctionContext functionContext) throws Exception {
		Core.TypeMessage message = Core.TypeMessage.parseFrom(functionContext.getFunctionContext().getValue());

		Map<String, Trigger> triggers = new HashMap<>();

		try {
			switch (message.getMethod()) {
			case CREATE:
				triggers = Trigger.getByType(functionContext.getDocument(), Trigger.CREATE);
				break;

			case UPDATE:
				triggers = Trigger.getByType(functionContext.getDocument(), Trigger.UPDATE);
				break;

			case DELETE:
				triggers = Trigger.getByType(functionContext.getDocument(), Trigger.DELETE);
				break;

			default:
				throw new UnknownMethodException(message.getMethod());
			}

			for (Trigger trigger : triggers.values()) {
				Functions.FunctionContext pbFunctionContext = Exec(functionContext.getFlinkContext().caller().id(),
						trigger.getNamespace(), trigger.getType(), message.getMethod());

				TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

				FunctionType functionType = new FunctionType(trigger.getNamespace(), trigger.getType());

				functionContext.getFlinkContext().send(functionType, functionContext.getFlinkContext().caller().id(),
						typedValue);
			}
		} catch (TriggerNotFoundException ignore) {
		}
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

		Core.TypeMessage typeMessage = Core.TypeMessage.newBuilder().setMethod(method).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(id).setValue(typeMessage.toByteString());
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

		Core.TypeMessage typeMessage = Core.TypeMessage.newBuilder().setMethod(method).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(id).setValue(typeMessage.toByteString());
		return builder.build();
	}
}
