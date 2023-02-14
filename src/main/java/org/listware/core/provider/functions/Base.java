/*
 *  Copyright 2023 NJWS Inc.
 *  Copyright 2022 Listware
 */

package org.listware.core.provider.functions;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.core.FunctionContext;
import org.listware.core.cmdb.Cmdb;
import org.listware.io.utils.Constants;
import org.listware.sdk.Functions;
import org.listware.sdk.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class Base extends Sync implements StatefulFunction {
	private static final Logger LOG = LoggerFactory.getLogger(Base.class);

	protected Cmdb cmdb = new Cmdb();

	public Base(String groupID, String topic) {
		super(groupID, topic);
	}

	@Override
	public void invoke(Context context, java.lang.Object input) {
		long startTime = System.currentTimeMillis();

		try {
			LOG.debug(context.self().toString());
			if (input instanceof TypedValue) {
				TypedValue typedValue = (TypedValue) input;
				if (typedValue.getTypename().equals(Constants.MESSAGE_TYPENAME)) {
					Functions.FunctionContext functionContext = Functions.FunctionContext
							.parseFrom(typedValue.getValue());

					onInit(context, functionContext);
					invoke(context, functionContext);
				} else if (typedValue.getTypename().equals(Constants.RESULT_MESSAGE_TYPENAME)) {
					Result.FunctionResult functionResult = Result.FunctionResult.parseFrom(typedValue.getValue());
					onResult(context, functionResult);
				} else {
					LOG.error(context.self() + " unknown type received: " + typedValue.getTypename());
				}

			} else {
				LOG.error(context.self() + " unknown message received: " + input);
			}
		} catch (Exception e) {
			LOG.error(context.self() + " " + e.getLocalizedMessage());
			onException(context, e.getLocalizedMessage());
		} finally {
			try {
				onReply(context);

				long endTime = System.currentTimeMillis();

				LOG.debug(context.self() + ": took " + (endTime - startTime) + " milliseconds");
			} catch (Exception e) {
				LOG.error(context.self() + ": " + e.getLocalizedMessage());
			}
		}
	}

	public void invoke(Context context, Functions.FunctionContext functionContext) throws Exception {
	}

	public void invoke(FunctionContext functionContext) throws Exception {
	}

	/**
	 * CreateFunctionContext function
	 **
	 * @param id        string
	 * @param namespace string
	 * @param type      string
	 * @param method    Method
	 */
	public static Functions.FunctionContext CreateFunctionContext(String id, String namespace, String type) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(namespace).setType(type)
				.build();
		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(id);
		return builder.build();
	}

	public static Functions.FunctionContext CreateFunctionContext(String id, String namespace, String type,
			ByteString payload) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(namespace).setType(type)
				.build();
		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(id).setValue(payload);
		return builder.build();
	}

}
