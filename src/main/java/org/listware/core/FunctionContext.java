/* Copyright 2022 Listware */

package org.listware.core;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import org.listware.sdk.Functions;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.utils.Cmdb.Matcher;
import org.listware.core.provider.utils.Cmdb.SystemKeys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionContext {
	private static final Logger LOG = LoggerFactory.getLogger(FunctionContext.class);

	private Context flinkContext;
	private ObjectDocument document;
	private Functions.FunctionContext functionContext;
	private boolean isExecutedCallback = false;

	public FunctionContext(Context context, ObjectDocument document, Functions.FunctionContext functionContext) {
		this.flinkContext = context;
		this.document = document;
		this.functionContext = functionContext;
	}

	public Context getFlinkContext() {
		return flinkContext;
	}

	public ObjectDocument getDocument() {
		return document;
	}

	public Functions.FunctionContext getFunctionContext() {
		return functionContext;
	}

	/**
	 * root
	 **
	 */
	public boolean isRoot() {
		return document.getKey().equals(SystemKeys.ROOT);
	}

	/**
	 * objects.root
	 **
	 */
	public boolean isObjects() {
		return document.getKey().equals(SystemKeys.OBJECTS);
	}

	/**
	 * types.root
	 **
	 */
	public boolean isTypes() {
		return document.getKey().equals(SystemKeys.TYPES);
	}

	/**
	 * is link?
	 **
	 */
	public boolean isLink() {
		return document.getKey().matches(Matcher.NUMERIC_STRING);
	}

	/**
	 * is object?
	 **
	 */
	public boolean isObject() {
		return document.getKey().matches(Matcher.UUID_V4_STRING);
	}

	/**
	 * is type?
	 **
	 */
	public boolean isType() {
		return (!isRoot() && !isObjects() && !isTypes() && !isLink() && !isObject());
	}

	// You can execute callback only once, getCallback() to inherit func or
	// Callback() after invoke
	public void callback() throws Exception {
		Functions.FunctionContext callback = getCallback();
		if (callback != null) {
			String namespace = callback.getFunctionType().getNamespace();
			String type = callback.getFunctionType().getType();
			FunctionType functionType = new FunctionType(namespace, type);

			LOG.info("send: " + functionType + " id " + callback.getId());

			TypedValue typedValue = TypedValueDeserializer.fromMessageLite(callback);
			;

			flinkContext.send(functionType, callback.getId(), typedValue);
		}
	}

	public Functions.FunctionContext getCallback() {
		if (functionContext.hasCallback() && !isExecutedCallback) {
			isExecutedCallback = true;
			return functionContext.getCallback();
		}
		return null;
	}
}
