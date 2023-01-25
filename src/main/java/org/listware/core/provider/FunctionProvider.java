/* Copyright 2022 Listware */

package org.listware.core.provider;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.listware.core.cmdb.Cmdb;
import org.listware.core.provider.functions.Log;
import org.listware.core.provider.functions.Register;
import org.listware.core.provider.functions.Router;
import org.listware.core.provider.functions.link.AdvancedLink;
import org.listware.core.provider.functions.link.LinkTrigger;
import org.listware.core.provider.functions.object.Link;
import org.listware.core.provider.functions.object.Object;
import org.listware.core.provider.functions.object.ObjectTrigger;
import org.listware.core.provider.functions.object.Type;
import org.listware.core.provider.functions.object.TypeTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionProvider implements StatefulFunctionProvider {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(FunctionProvider.class);

	private Log log = new Log();
	private Register register = new Register();
	private Object object = new Object();
	private ObjectTrigger objectTrigger = new ObjectTrigger();
	private Type type = new Type();
	private TypeTrigger typeTrigger = new TypeTrigger();
	private Link link = new Link();
	private AdvancedLink advancedlink = new AdvancedLink();
	private LinkTrigger linkTrigger = new LinkTrigger();
	private Router router = new Router();

	public FunctionProvider() {
		try {
			Cmdb cmdb = new Cmdb();
			cmdb.bootstrap();
			cmdb.shutdown();
		} catch (Exception e) {
			LOG.error(e.getLocalizedMessage());
		}
	}

	@Override
	public StatefulFunction functionOfType(FunctionType functionType) {
		if (functionType.equals(Router.FUNCTION_TYPE)) {
			return router;
		}

		if (functionType.equals(Type.FUNCTION_TYPE)) {
			return type;
		}
		if (functionType.equals(TypeTrigger.FUNCTION_TYPE)) {
			return typeTrigger;
		}

		if (functionType.equals(Object.FUNCTION_TYPE)) {
			return object;
		}
		if (functionType.equals(ObjectTrigger.FUNCTION_TYPE)) {
			return objectTrigger;
		}

		if (functionType.equals(Link.FUNCTION_TYPE)) {
			return link;
		}

		if (functionType.equals(Log.FUNCTION_TYPE)) {
			return log;
		}

		if (functionType.equals(Register.FUNCTION_TYPE)) {
			return register;
		}

		if (functionType.equals(AdvancedLink.FUNCTION_TYPE)) {
			return advancedlink;
		}
		
		if (functionType.equals(LinkTrigger.FUNCTION_TYPE)) {
			return linkTrigger;
		}
		
		return null;
	}
}
