/* Copyright 2022 Listware */

package org.listware.core.provider.utils.exceptions;

public class AlreadyTriggerException extends Exception {
	private static final long serialVersionUID = 1L;

	public AlreadyTriggerException() {
		super("trigger already exists");
	}
}
