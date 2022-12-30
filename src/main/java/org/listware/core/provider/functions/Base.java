/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.io.functions.egress.Egress;
import org.listware.sdk.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Base implements StatefulFunction {
	private static final Logger LOG = LoggerFactory.getLogger(Base.class);

	@Override
	public void invoke(Context context, java.lang.Object input) {
		long startTime = System.currentTimeMillis();

		LOG.info(context.self().toString());

		if (!(input instanceof TypedValue)) {
			LOG.error("unknown message received: " + input);
			return;
		}

		Functions.FunctionResult.Builder functionResultBuilder = Functions.FunctionResult.newBuilder();

		TypedValue typedValue = (TypedValue) input;

		Functions.FunctionContext functionContext = null;

		try {
			functionContext = Functions.FunctionContext.parseFrom(typedValue.getValue());

			invoke(context, functionContext);

			functionResultBuilder.setComplete(true);
		} catch (Exception e) {
			LOG.error(e.getLocalizedMessage());
			functionResultBuilder.setError(e.getLocalizedMessage());
		} finally {
			if (functionContext.hasReplyEgress()) {
				Functions.ReplyEgress replyEgress = functionContext.getReplyEgress();

				Functions.FunctionResult functionResult = functionResultBuilder.setReplyEgress(replyEgress).build();

				TypedValue newTypedValue = TypedValue.newBuilder().setValue(functionResult.toByteString())
						.setHasValue(true).build();

				LOG.info("ReplyEgress result: " + replyEgress.getId());

				context.send(Egress.EGRESS, newTypedValue);
			}

			long endTime = System.currentTimeMillis();

			LOG.info(context.self().type() + ": took " + (endTime - startTime) + " milliseconds");
		}

	}

	public void invoke(Context context, Functions.FunctionContext functionContext) throws Exception {

	}

}
