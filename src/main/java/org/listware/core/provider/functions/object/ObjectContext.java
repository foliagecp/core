/* Copyright 2022 Listware */

package org.listware.core.provider.functions.object;

import org.apache.flink.statefun.sdk.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.listware.core.FunctionContext;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.functions.Base;
import org.listware.sdk.Functions;

/**
 * Middleware for CUD functions
 **
 */
public class ObjectContext extends Base {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(ObjectContext.class);


	public ObjectContext(String groupID, String topic) {
		super(groupID, topic);
	}

	@Override
	public void invoke(Context context, Functions.FunctionContext pbFunctionContext) throws Exception {
		ObjectDocument document = cmdb.readDocument(context.self().id());

		FunctionContext functionContext = new FunctionContext(context, document, pbFunctionContext);

		invoke(functionContext);
	}

}
