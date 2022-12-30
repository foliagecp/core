/* Copyright 2022 Listware */

package org.listware.core.provider.utils;

public class Cmdb {
	// Constant system cmdb keys
	public class SystemKeys {
		public static final String ROOT = "root";
		public static final String OBJECTS = "objects";
		public static final String TYPES = "types";
	}

	// Collection names
	public class Collections {
		public static final String SYSTEM = "system";
		public static final String TYPES = "types";
		public static final String OBJECTS = "objects";
		public static final String LINKS = "links";
	}

	// Link entries
	public class LinkTypes {
		public static final String TYPE = "type";
	}

	public class Matcher {
		public static final String UUID_V4_STRING = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[89abAB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}";
		public static final String NUMERIC_STRING = "\\d+";
	}
}
