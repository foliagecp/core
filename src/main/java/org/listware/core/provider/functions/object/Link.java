/* Copyright 2022 Listware */

package org.listware.core.provider.functions.object;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.listware.core.FunctionContext;
import org.listware.core.cmdb.Trigger;
import org.listware.core.documents.LinkDocument;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.functions.link.AdvancedLink;
import org.listware.core.utils.exceptions.AlreadyTriggerException;
import org.listware.core.utils.exceptions.UnknownIdException;
import org.listware.core.utils.exceptions.UnknownMethodException;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.Result;
import org.listware.sdk.pbcmdb.Core;

/**
 * Link CUD arangodb service for links
 **
 */
public class Link extends ObjectContext {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Link.class);

	public static final String TYPE = "links.system.functions.root";

	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	public Link() {
		super(TYPE, TYPE);
	}

	@Override
	public void invoke(FunctionContext functionContext) throws Exception {
		Core.LinkMessage message = Core.LinkMessage.parseFrom(functionContext.getFunctionContext().getValue());

		switch (message.getMethod()) {
		case CREATE:
			create(functionContext, message);
			break;

		case UPDATE:
			update(functionContext, message);
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

	private void create(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		cmdb.checkFrom(functionContext.getDocument(), message.getName());

		ObjectDocument document = cmdb.readDocument(message.getTo());

		cmdb.createLink(functionContext.getFlinkContext(), functionContext.getDocument(), document, message.getType(),
				message.getName(), message.getPayload());
	}

	private void update(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		Result.ReplyResult replyResult = replyResult(functionContext.getFlinkContext());

		LinkDocument document = cmdb.readLinkDocument(functionContext.getDocument().getId(), message.getName());

		Functions.FunctionContext pbFunctionContext = AdvancedLink.ProxyMessage(document.getId(), message, replyResult);

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

		functionContext.getFlinkContext().send(AdvancedLink.FUNCTION_TYPE, document.getId(), typedValue);
	}

	private void delete(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		Result.ReplyResult replyResult = replyResult(functionContext.getFlinkContext());

		LinkDocument document = cmdb.readLinkDocument(functionContext.getDocument().getId(), message.getName());

		Functions.FunctionContext pbFunctionContext = AdvancedLink.ProxyMessage(document.getId(), message, replyResult);

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

		functionContext.getFlinkContext().send(AdvancedLink.FUNCTION_TYPE, document.getId(), typedValue);
	}

	private void createTrigger(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		if (!functionContext.isType()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}

		Core.Trigger trigger = Core.Trigger.parseFrom(message.getPayload());

		ObjectDocument from = functionContext.getDocument();
		ObjectDocument to = cmdb.readDocument(message.getTo());

		LinkDocument document = null;
		try {
			document = cmdb.readLinkDocument(from.getId(), to.getKey());
			document = Trigger.add(document, trigger);
			document = cmdb.updateLinkDocument(document);
		} catch (AlreadyTriggerException ex) {
			throw ex;
		} catch (Exception ex) {
			document = Trigger.add(new LinkDocument(), trigger);
			document = cmdb.createLink(from, to, "trigger", to.getKey(), document.serialize());
		}

		LOG.info("created link trigger " + document.getId());
	}

	private void deleteTrigger(FunctionContext functionContext, Core.LinkMessage message) throws Exception {
		if (!functionContext.isType()) {
			throw new UnknownIdException(functionContext.getFlinkContext().self().id());
		}

		Core.Trigger trigger = Core.Trigger.parseFrom(message.getPayload());

		ObjectDocument from = functionContext.getDocument();
		ObjectDocument to = cmdb.readDocument(message.getTo());

		LinkDocument document = cmdb.readLinkDocument(from.getId(), to.getKey());
		document = Trigger.delete(document, trigger);
		document = cmdb.updateLinkDocument(document);

		LOG.info("deleted type trigger " + document.getId());
	}

	/**
	 * CreateLink create link 'from' -> 'to' with 'name'
	 **
	 * @param from will be ('system/root', 'types/node', '/$uuid')
	 * @param to   will be ('system/root', 'types/node', '/$uuid')
	 * @param name string
	 */
	public static Functions.FunctionContext CreateLink(String from, String to, String name) {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(Link.TYPE).build();

		Core.LinkMessage linkMessage = Core.LinkMessage.newBuilder().setMethod(Core.Method.CREATE).setName(name)
				.setTo(to).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setFunctionType(functionType)
				.setId(from).setValue(linkMessage.toByteString());
		return builder.build();
	}
}
