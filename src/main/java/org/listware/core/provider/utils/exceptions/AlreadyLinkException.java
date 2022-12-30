/* Copyright 2022 Listware */

package org.listware.core.provider.utils.exceptions;

public class AlreadyLinkException extends Exception  {
	private static final long serialVersionUID = 1L;

	public AlreadyLinkException(String from, String name) {
		super(String.format("link %s -> %s  already exists: ", from, name));
	}
}
