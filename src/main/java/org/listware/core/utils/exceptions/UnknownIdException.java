/* Copyright 2022 Listware */

package org.listware.core.utils.exceptions;

public class UnknownIdException extends Exception {

	private static final long serialVersionUID = 1L;

	public UnknownIdException(String id) {
		super(String.format("unknown id '%s'", id));
	}
}
