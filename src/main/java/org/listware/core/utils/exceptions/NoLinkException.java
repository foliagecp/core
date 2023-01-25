/* Copyright 2022 Listware */

package org.listware.core.utils.exceptions;

public class NoLinkException extends Exception {
	private static final long serialVersionUID = 1L;

	public NoLinkException(String from, String name) {
		super(String.format("link %s -> %s  not found: ", from, name));
	}

	public NoLinkException(String from) {
		super(String.format("link from %s not found: ", from));
	}
}
