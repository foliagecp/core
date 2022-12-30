/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log implements StatefulFunction {
	private static final Logger LOG = LoggerFactory.getLogger(Log.class);

	public static final String TYPE = "log.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	@Override
	public void invoke(Context context, java.lang.Object input) {
		LOG.info(context.self().toString());

		long startTime = System.currentTimeMillis();

		try {
			middleware(context, input);
		} catch (Exception e) {
			LOG.error(e.getLocalizedMessage());
		}
		long endTime = System.currentTimeMillis();

		LOG.info(context.self().type() + ": took " + (endTime - startTime) + " milliseconds");
	}

	@SuppressWarnings("unused")
	private void middleware(Context context, java.lang.Object input) throws Exception {
		LOG.info("LOG: called from " + context.caller().toString());

		if (!(input instanceof TypedValue)) {
			throw new IllegalArgumentException("unknown message received " + input);
		}

		TypedValue value = (TypedValue) input;

		// TODO use message
		Functions.FunctionContext functionContext = Functions.FunctionContext.parseFrom(value.getValue());
	}

}
