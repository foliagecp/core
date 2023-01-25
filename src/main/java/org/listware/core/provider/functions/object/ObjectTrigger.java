/* Copyright 2022 Listware */

package org.listware.core.provider.functions.object;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.core.FunctionContext;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectTrigger extends ObjectContext {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(ObjectTrigger.class);

	private static final String TYPE = "trigger.objects.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public ObjectTrigger() {
		super(TYPE, TYPE);
	}

	@Override
	public void invoke(FunctionContext functionContext) throws Exception {
		Core.ObjectMessage message = Core.ObjectMessage.parseFrom(functionContext.getFunctionContext().getValue());

		String typeId = cmdb.getTypeId(functionContext.getFlinkContext().self().id());
		Functions.FunctionContext pbFunctionContext = TypeTrigger.Trigger(typeId, message.getMethod());

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

		functionContext.getFlinkContext().send(TypeTrigger.FUNCTION_TYPE, pbFunctionContext.getId(), typedValue);
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

		Core.ObjectMessage message = Core.ObjectMessage.newBuilder().setMethod(method).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(id).setValue(message.toByteString());
		return builder.build();
	}
}
