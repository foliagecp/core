/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.core.FunctionContext;
import org.listware.core.cmdb.Cmdb;
import org.listware.sdk.Functions;
import org.listware.sdk.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			LOG.info(context.self().toString());

			if (input instanceof TypedValue) {
				TypedValue typedValue = (TypedValue) input;

				Functions.FunctionContext functionContext = Functions.FunctionContext.parseFrom(typedValue.getValue());

				onInit(context, functionContext);
				invoke(context, functionContext);

			} else if (input instanceof Result.FunctionResult) {
				Result.FunctionResult functionResult = (Result.FunctionResult) input;
				onResult(context, functionResult);

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

				LOG.info(context.self() + ": took " + (endTime - startTime) + " milliseconds");
			} catch (Exception e) {
				LOG.error(context.self() + ": " + e.getLocalizedMessage());
			}
		}
	}

	public void invoke(Context context, Functions.FunctionContext functionContext) throws Exception {
	}

	public void invoke(FunctionContext functionContext) throws Exception {
	}

}
