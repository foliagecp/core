/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import java.util.Map;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.listware.core.FunctionContext;
import org.listware.core.provider.utils.Trigger;
import org.listware.core.provider.utils.exceptions.UnknownMethodException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoGraph;

public class TypeTrigger extends Arango {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(TypeTrigger.class);

	private static final String TYPE = "trigger.types.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public TypeTrigger(ArangoGraph graph) {
		super(graph);
	}

	@Override
	public void invoke(FunctionContext functionContext) throws Exception {
		Core.TypeMessage message = Core.TypeMessage.parseFrom(functionContext.getFunctionContext().getValue());

		switch (message.getMethod()) {
		case CREATE:
			try {
				Map<String, Trigger> create = Trigger.getByType(functionContext.getDocument(), "create");

				for (Trigger trigger : create.values()) {
					Functions.FunctionContext pbFunctionContext = Arango.Exec(
							functionContext.getFlinkContext().caller().id(), trigger.getNamespace(), trigger.getType(),
							message.getMethod());

					TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

					FunctionType functionType = new FunctionType(trigger.getNamespace(), trigger.getType());

					functionContext.getFlinkContext().send(functionType,
							functionContext.getFlinkContext().caller().id(), typedValue);
				}
			} catch (Exception ex) {

			}

			break;

		case UPDATE:
			break;

		case DELETE:
			break;

		default:
			throw new UnknownMethodException(message.getMethod());
		}
	}

	/**
	 * Trigger trigger
	 **
	 * @param type   string
	 * @param method Method
	 */
	public static Functions.FunctionContext Trigger(String type, Core.Method method) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(TYPE).build();

		Core.TypeMessage typeMessage = Core.TypeMessage.newBuilder().setMethod(method).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(type).setValue(typeMessage.toByteString());
		return builder.build();
	}
}
