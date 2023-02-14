/*
 *  Copyright 2023 NJWS Inc.
 *  Copyright 2022 Listware
 */

package org.listware.core.provider.functions.link;

import org.apache.flink.statefun.sdk.FunctionType;
import org.listware.core.FunctionContext;
import org.listware.core.cmdb.Trigger;
import org.listware.core.documents.LinkDocument;
import org.listware.core.utils.exceptions.UnknownMethodException;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.Result;
import org.listware.sdk.pbcmdb.Core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Link CUD arangodb service for links
 **
 */
public class AdvancedLink extends LinkContext {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(AdvancedLink.class);

	public static final String TYPE = "advanced.links.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public AdvancedLink() {
		super(TYPE, TYPE);
	}

	@Override
	public void invoke(FunctionContext functionContext) throws Exception {
		Core.LinkMessage message = Core.LinkMessage.parseFrom(functionContext.getFunctionContext().getValue());

		switch (message.getMethod()) {
		case UPDATE:
			update(functionContext, message);
			break;

		case REPLACE:
			replace(functionContext, message);
			break;

		case DELETE:
			delete(functionContext, message);
			break;

		case CREATE_TRIGGER:
			createTrigger(functionContext, message);
			break;

		case DELETE_TRIGGER:
			deleteTrigger(functionContext, message);
			break;

		default:
			throw new UnknownMethodException(message.getMethod());
		}
	}

	private void update(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		LinkDocument document = LinkDocument.deserialize(message.getPayload());
		document.setId(functionContext.getFlinkContext().self().id());

		document = cmdb.updateLink(functionContext.getFlinkContext(), document);
	}

	private void replace(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		LinkDocument document = LinkDocument.deserialize(message.getPayload());
		document.setId(functionContext.getFlinkContext().self().id());

		document = cmdb.replaceLink(functionContext.getFlinkContext(), document);
	}

	private void delete(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		LinkDocument document = (LinkDocument) functionContext.getDocument();
		document.setId(functionContext.getFlinkContext().self().id());

		cmdb.removeDocument(document);
	}

	private void createTrigger(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		LinkDocument document = (LinkDocument) functionContext.getDocument();

		Core.Trigger trigger = Core.Trigger.parseFrom(message.getPayload());

		document = Trigger.add(document, trigger);
		document = cmdb.updateLinkDocument(document);

		LOG.debug("created link trigger " + document.getId());
	}

	private void deleteTrigger(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		LinkDocument document = (LinkDocument) functionContext.getDocument();

		Core.Trigger trigger = Core.Trigger.parseFrom(message.getPayload());

		document = Trigger.delete(document, trigger);
		document = cmdb.updateLinkDocument(document);

		LOG.debug("deleted type trigger " + document.getId());
	}

	public static Functions.FunctionContext ProxyMessage(String id, Core.LinkMessage linkMessage,
			Result.ReplyResult replyResult) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(AdvancedLink.TYPE).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(id).setValue(linkMessage.toByteString());
		if (replyResult != null) {
			builder = builder.setReplyResult(replyResult);
		}
		return builder.build();
	}
}