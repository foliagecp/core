/* Copyright 2022 Listware */

package org.listware.core.utils.exceptions;

public class TriggerNotFoundException extends Exception {
	private static final long serialVersionUID = 1L;

	public TriggerNotFoundException() {
		super("trigger not found");
	}
}
