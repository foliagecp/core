/* Copyright 2022 Listware */

package org.listware.core.utils;

import java.util.ArrayList;
import java.util.List;

import org.listware.io.functions.result.EgressReader;
import org.listware.sdk.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorContainer {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(ErrorContainer.class);

	private List<String> errors = new ArrayList<String>();
	private Boolean complete = true;

	public ErrorContainer() {
		// POJO
	}

	public Boolean getComplete() {
		return complete;
	}

	public void setComplete(Boolean complete) {
		this.complete = complete;
	}

	public List<String> getErrors() {
		return errors;
	}

	public void setErrors(List<String> errors) {
		this.errors = errors;
	}

	public void append(String error) {
		errors.add(error);
		complete = false;
	}

	public void appendAll(List<String> errors) {
		for (String error : errors) {
			append(error);
		}
	}

	public Result.FunctionResult toFunctionResult(EgressReader.ReplyResult replyEgress) {
		Result.FunctionResult.Builder functionResultBuilder = Result.FunctionResult.newBuilder();
		functionResultBuilder.setComplete(getComplete());
		functionResultBuilder.addAllErrors(getErrors());
		Result.FunctionResult functionResult = functionResultBuilder.setReplyEgress(replyEgress.toProto()).build();
		return functionResult;
	}

}
