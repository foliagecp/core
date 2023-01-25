/* Copyright 2022 Listware */

package org.listware.core.utils.exceptions;

import org.listware.sdk.pbcmdb.Core;

public class UnknownMethodException extends Exception {
	private static final long serialVersionUID = 1L;

	public UnknownMethodException(Core.Method method) {
		super(String.format("unknown method '%s'", method.toString()));
	}
}
