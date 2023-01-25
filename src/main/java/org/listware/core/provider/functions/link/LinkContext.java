/* Copyright 2022 Listware */

package org.listware.core.provider.functions.link;

import org.apache.flink.statefun.sdk.Context;
import org.listware.core.FunctionContext;
import org.listware.core.documents.LinkDocument;
import org.listware.core.provider.functions.Base;
import org.listware.sdk.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Middleware for CUD functions
 **
 */
public class LinkContext extends Base {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(LinkContext.class);

	public LinkContext(String groupID, String topic) {
		super(groupID, topic);
	}

	@Override
	public void invoke(Context context, Functions.FunctionContext pbFunctionContext) throws Exception {
		LinkDocument document = cmdb.readLinkDocument(context.self().id());

		FunctionContext functionContext = new FunctionContext(context, document, pbFunctionContext);

		invoke(functionContext);
	}

}
