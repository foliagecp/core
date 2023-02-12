/* Copyright 2022 Listware */

package org.listware.core.provider.functions;

import java.util.Iterator;
import java.util.UUID;

import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.listware.core.utils.ErrorContainer;
import org.listware.io.functions.result.Egress;
import org.listware.io.functions.result.EgressReader;
import org.listware.io.utils.Constants;
import org.listware.sdk.Functions;
import org.listware.sdk.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sync {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Sync.class);

	private static final String RESULT_TABLE = "result-table";
	private static final String REPLY_TABLE = "reply-table";
	private static final String ERRORS_TABLE = "errors-table";

	@Persisted
	private PersistedTable<String, EgressReader.ReplyResult> replyTable = PersistedTable.of(REPLY_TABLE, String.class,
			EgressReader.ReplyResult.class);

	@Persisted
	private PersistedTable<String, ErrorContainer> errorsTable = PersistedTable.of(ERRORS_TABLE, String.class,
			ErrorContainer.class);

	@Persisted
	private PersistedTable<String, String> resultTable = PersistedTable.of(RESULT_TABLE, String.class, String.class);

	private EgressReader egressReader = null;

	protected String key = null;

	protected ErrorContainer errorContainer = null;

	public Sync(String groupID, String topic) {
		egressReader = new EgressReader(groupID, topic);
		UUID uuid = UUID.randomUUID();
		key = uuid.toString();
	}

	protected Result.ReplyResult replyResult(Context context) {
		Result.ReplyResult replyResult = egressReader.replyResult(context.self().id());

		String key = replyResult.getKey();

		resultTable.set(key, this.key);

		return replyResult;
	}

	protected void onInit(Context context, Functions.FunctionContext functionContext) throws Exception {
		if (functionContext.hasReplyResult()) {
			EgressReader.ReplyResult replyResult = new EgressReader.ReplyResult(functionContext.getReplyResult());

			if (context.caller() != null) {
				replyResult.setIsEgress(false);
			} else {
				replyResult.setIsEgress(true);
			}

			this.key = replyResult.getKey();

			replyTable.set(this.key, replyResult);
		} else {
			UUID uuid = UUID.randomUUID();
			key = uuid.toString();
		}
		errorContainer = new ErrorContainer();
		errorsTable.set(this.key, errorContainer);
	}

	protected void onResult(Context context, Result.FunctionResult functionResult) throws Exception {
		String key = functionResult.getReplyEgress().getKey();

		this.key = resultTable.get(key);
		resultTable.remove(key);

		errorContainer = errorsTable.get(this.key);

		if (!functionResult.getComplete()) {
			if (errorContainer != null) {
				errorContainer.appendAll(functionResult.getErrorsList());
				errorsTable.set(this.key, errorContainer);
			}
		}
	}

	protected void onException(Context context, String message) {
		errorContainer = errorsTable.get(this.key);
		if (errorContainer != null) {
			errorContainer.append(message);
			errorsTable.set(this.key, errorContainer);
		}
	}

	protected void onReply(Context context) throws Exception {
		errorContainer = errorsTable.get(this.key);
		if (errorContainer == null) {
			return;
		}

		Iterator<String> it = resultTable.values().iterator();

		while (it.hasNext()) {
			if (it.next().equals(this.key)) {
				return;
			}
		}

		errorsTable.remove(this.key);

		EgressReader.ReplyResult replyResult = replyTable.get(this.key);
		if (replyResult == null) {
			return;
		}

		replyTable.remove(this.key);

		reply(context, replyResult, errorContainer);
	}

	protected void reply(Context context, EgressReader.ReplyResult replyResult, ErrorContainer errorContainer)
			throws Exception {
		Result.FunctionResult functionResult = errorContainer.toFunctionResult(replyResult);

		if (!replyResult.getIsEgress()) {
			Address address = replyResult.toAddress();

			TypedValue typedValue = TypedValue.newBuilder().setValue(functionResult.toByteString())
					.setTypename(Constants.RESULT_MESSAGE_TYPENAME).setHasValue(true).build();

			// send result to caller
			context.send(address, typedValue);
		} else {
			KafkaProducerRecord kafkaProducerRecord = KafkaProducerRecord.newBuilder().setTopic(replyResult.getTopic())
					.setKey(replyResult.getKey()).setValueBytes(functionResult.toByteString()).build();

			TypedValue typedValue = TypedValue.newBuilder().setValue(kafkaProducerRecord.toByteString())
					.setTypename(Constants.RESULT_MESSAGE_TYPENAME).setHasValue(true).build();
			context.send(Egress.EGRESS, typedValue);
		}
	}

}
