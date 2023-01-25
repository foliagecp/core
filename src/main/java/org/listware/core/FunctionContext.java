/* Copyright 2022 Listware */

package org.listware.core;

import org.apache.flink.statefun.sdk.Context;

import org.listware.sdk.Functions;
import org.listware.core.cmdb.Cmdb.SystemKeys;
import org.listware.core.documents.ObjectDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionContext {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(FunctionContext.class);

	private Context flinkContext;
	private ObjectDocument document;
	private Functions.FunctionContext functionContext;


	private class Matcher {
		public static final String UUID_V4_STRING = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[89abAB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}";
		public static final String NUMERIC_STRING = "\\d+";
	}

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
}