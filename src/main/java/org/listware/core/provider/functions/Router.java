/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.listware.io.grpc.QDSLClient;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.Result;
import org.listware.sdk.pbcmdb.pbqdsl.QDSL;

/**
 * Function router by qdsl
 **
 */
public class Router extends Base {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Router.class);

	public static final String TYPE = "router.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	private QDSLClient client = new QDSLClient();

	public Router() {
		super(TYPE, TYPE);
	}

	@Override
	public void invoke(Context context, Functions.FunctionContext functionContext) throws Exception {
		QDSL.Options options = QDSL.Options.newBuilder().setId(true).build();

		QDSL.Elements elements = client.qdsl(context.self().id(), options);

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.parseFrom(functionContext.getValue())
				.toBuilder();

		for (QDSL.Element element : elements.getElementsList()) {
			Result.ReplyResult replyResult = replyResult(context);

			Functions.FunctionContext newFunctionContext = builder.setReplyResult(replyResult).setId(element.getId())
					.build();

			String namespace = newFunctionContext.getFunctionType().getNamespace();
			String type = newFunctionContext.getFunctionType().getType();
			FunctionType functionType = new FunctionType(namespace, type);

			TypedValue typedValue = TypedValueDeserializer.fromMessageLite(newFunctionContext);

			context.send(functionType, newFunctionContext.getId(), typedValue);
		}
	}

}
