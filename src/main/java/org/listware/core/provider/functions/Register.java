/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.listware.core.cmdb.RegisterMessage;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.Result;
import org.listware.sdk.pbcmdb.Core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.listware.core.provider.functions.object.Type;
import org.listware.core.provider.functions.object.Object;
import org.listware.core.provider.functions.object.Link;

public class Register extends Base {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Register.class);

	public static final String TYPE = "register.system.functions.root";
	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	private static final String MESSAGES_TABLE = "messages-table";
	private static final String STATE_TABLE = "types-state-table";

	@Persisted
	private PersistedTable<String, RegisterMessage> messagesTable = PersistedTable.of(MESSAGES_TABLE, String.class,
			RegisterMessage.class);

	@Persisted
	private PersistedTable<String, Boolean> stateTable = PersistedTable.of(STATE_TABLE, String.class, Boolean.class);

	public Register() {
		super(TYPE, TYPE);
	}

	@Override
	public void invoke(Context context, Functions.FunctionContext functionContext) throws Exception {
		Core.RegisterMessage registerMessage = Core.RegisterMessage.parseFrom(functionContext.getValue());

		RegisterMessage message = new RegisterMessage(registerMessage);
		messagesTable.set(this.key, message);

		registerNext(context);
	}

	private void registerRouter(Context context, Functions.FunctionContext.Builder builder) throws Exception {
		Result.ReplyResult replyResult = replyResult(context);
		stateTable.set(replyResult.getKey(), true);

		Functions.FunctionContext functionContext = builder.build();

		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(Router.TYPE).build();

		Functions.FunctionContext routerFunctionContext = Functions.FunctionContext.newBuilder()
				.setReplyResult(replyResult).setId(functionContext.getId()).setFunctionType(functionType)
				.setValue(functionContext.toByteString()).build();

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(routerFunctionContext);

		context.send(Router.FUNCTION_TYPE, routerFunctionContext.getId(), typedValue);
	}

	private void registerBuilder(Context context, FunctionType functionType, Functions.FunctionContext.Builder builder)
			throws Exception {
		Result.ReplyResult replyResult = replyResult(context);
		stateTable.set(replyResult.getKey(), true);

		Functions.FunctionContext functionContext = builder.setReplyResult(replyResult).build();
		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(functionContext);

		context.send(functionType, functionContext.getId(), typedValue);
	}

	private void registerType(Context context, Core.RegisterTypeMessage message) throws Exception {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(Type.TYPE).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setId(message.getId())
				.setFunctionType(functionType).setValue(message.getTypeMessage().toByteString());

		if (message.getRouter()) {
			registerRouter(context, builder);
		} else {
			registerBuilder(context, Type.FUNCTION_TYPE, builder);
		}

	}

	private void registerObject(Context context, Core.RegisterObjectMessage message) throws Exception {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(Object.TYPE).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setId(message.getId())
				.setFunctionType(functionType).setValue(message.getObjectMessage().toByteString());

		if (message.getRouter()) {
			registerRouter(context, builder);
		} else {
			registerBuilder(context, Object.FUNCTION_TYPE, builder);
		}
	}

	private void registerLink(Context context, Core.RegisterLinkMessage message) throws Exception {
		Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
				.setType(Object.TYPE).build();

		Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder().setId(message.getId())
				.setFunctionType(functionType).setValue(message.getLinkMessage().toByteString());

		registerBuilder(context, Link.FUNCTION_TYPE, builder);
	}

	@Override
	protected void onResult(Context context, Result.FunctionResult functionResult) throws Exception {
		super.onResult(context, functionResult);

		String key = functionResult.getReplyEgress().getKey();

		stateTable.remove(key);

		registerNext(context);
	}

	@Override
	protected void onReply(Context context) throws Exception {
		RegisterMessage message = messagesTable.get(this.key);

		// wait all answers
		if (stateTable.keys().iterator().hasNext()) {
			return;
		}

		// if errors - reply answer
		if (errorContainer.getComplete()) {
			if (!message.getTypes().isEmpty()) {
				return;
			}

			if (!message.getObjects().isEmpty()) {
				return;
			}

			if (!message.getLinks().isEmpty()) {
				return;
			}
		}

		messagesTable.remove(this.key);
		super.onReply(context);
	}

	private void registerTypes(Context context) throws Exception {
		RegisterMessage message = messagesTable.get(this.key);

		for (Core.RegisterTypeMessage registerMessage : message.listTypes()) {
			registerType(context, registerMessage);
		}

		messagesTable.set(this.key, message);
	}

	private void registerObjects(Context context) throws Exception {
		RegisterMessage message = messagesTable.get(this.key);

		for (Core.RegisterObjectMessage registerMessage : message.listObjects()) {
			registerObject(context, registerMessage);
		}

		messagesTable.set(this.key, message);
	}

	private void registerLinks(Context context) throws Exception {
		RegisterMessage message = messagesTable.get(this.key);

		for (Core.RegisterLinkMessage registerMessage : message.listLinks()) {
			registerLink(context, registerMessage);
		}

		messagesTable.set(this.key, message);
	}

	private void registerNext(Context context) throws Exception {
		RegisterMessage message = messagesTable.get(this.key);

		if (stateTable.keys().iterator().hasNext()) {
			return;
		}

		if (!errorContainer.getComplete()) {
			return;
		}

		if (!message.getTypes().isEmpty()) {
			registerTypes(context);
		} else if (!message.getObjects().isEmpty()) {
			registerObjects(context);
		} else if (!message.getLinks().isEmpty()) {
			registerLinks(context);
		}

	}

}
