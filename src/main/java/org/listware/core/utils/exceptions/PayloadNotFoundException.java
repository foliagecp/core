/* Copyright 2022 Listware */

package org.listware.core.utils.exceptions;

public class PayloadNotFoundException extends Exception {
	private static final long serialVersionUID = 1L;

	public PayloadNotFoundException() {
		super("payload not found");
	}
}
