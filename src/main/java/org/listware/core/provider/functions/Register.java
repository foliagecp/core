/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.io.functions.egress.EgressReader;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.io.utils.Constants.Namespaces;
import org.listware.sdk.Functions;
import org.listware.sdk.Functions.ReplyEgress;
import org.listware.sdk.pbcmdb.Core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Register extends Base {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Register.class);

	public static final String TYPE = "register.system.functions.root";
	public static final FunctionType FUNCTION_TYPE = new FunctionType(Namespaces.INTERNAL, TYPE);

	private EgressReader egressReader = new EgressReader(TYPE, TYPE);

	@Override
	public void invoke(Context context, Functions.FunctionContext functionContext) throws Exception {
		Core.RegisterMessage registerMessage = Core.RegisterMessage.parseFrom(functionContext.getValue());

		registerTypes(context, registerMessage);

		registerObjects(context, registerMessage);
	}

	private void registerTypes(Context context, Core.RegisterMessage registerMessage) throws Exception {
		for (Core.RegisterTypeMessage registerTypeMessage : registerMessage.getTypeMessagesList()) {

			Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
					.setType(Type.TYPE).build();

			ReplyEgress replyEgress = egressReader.replyEgress();

			Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder()
					.setReplyEgress(replyEgress).setId(registerTypeMessage.getId()).setFunctionType(functionType)
					.setValue(registerTypeMessage.getTypeMessage().toByteString());

			Functions.FunctionContext newFunctionContext = builder.build();

			TypedValue newTypedValue = TypedValueDeserializer.fromMessageLite(newFunctionContext);

			context.send(Type.FUNCTION_TYPE, newFunctionContext.getId(), newTypedValue);

			egressReader.wait(replyEgress.getId());
		}
	}

	private void registerObjects(Context context, Core.RegisterMessage registerMessage) throws Exception {
		for (Core.RegisterObjectMessage registerObjectMessage : registerMessage.getObjectMessagesList()) {

			Functions.FunctionType functionType = Functions.FunctionType.newBuilder().setNamespace(Namespaces.INTERNAL)
					.setType(Object.TYPE).build();

			ReplyEgress replyEgress = egressReader.replyEgress();

			Functions.FunctionContext.Builder builder = Functions.FunctionContext.newBuilder()
					.setFunctionType(functionType).setId(registerObjectMessage.getId()).setReplyEgress(replyEgress)
					.setValue(registerObjectMessage.getObjectMessage().toByteString());

			Functions.FunctionContext newFunctionContext = builder.build();

			TypedValue newTypedValue = TypedValueDeserializer.fromMessageLite(newFunctionContext);

			context.send(Object.FUNCTION_TYPE, newFunctionContext.getId(), newTypedValue);

			egressReader.wait(replyEgress.getId());
		}
	}

}
