/* Copyright 2022 Listware */

package org.listware.core;

import java.util.Map;

import com.google.auto.service.AutoService;

import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.listware.core.provider.FunctionProvider;
import org.listware.core.provider.functions.Link;
import org.listware.core.provider.functions.Log;
import org.listware.core.provider.functions.Object;
import org.listware.core.provider.functions.ObjectTrigger;
import org.listware.core.provider.functions.Register;
import org.listware.core.provider.functions.Router;
import org.listware.core.provider.functions.Type;
import org.listware.core.provider.functions.TypeTrigger;

@AutoService(StatefulFunctionModule.class)
public final class Module implements StatefulFunctionModule {
	private FunctionProvider provider = new FunctionProvider();

	@Override
	public void configure(Map<String, String> globalConfiguration, Binder binder) {
		binder.bindFunctionProvider(Type.FUNCTION_TYPE, provider);
		binder.bindFunctionProvider(TypeTrigger.FUNCTION_TYPE, provider);
		binder.bindFunctionProvider(Object.FUNCTION_TYPE, provider);
		binder.bindFunctionProvider(ObjectTrigger.FUNCTION_TYPE, provider);
		binder.bindFunctionProvider(Link.FUNCTION_TYPE, provider);
		binder.bindFunctionProvider(Router.FUNCTION_TYPE, provider);
		binder.bindFunctionProvider(Log.FUNCTION_TYPE, provider);
		binder.bindFunctionProvider(Register.FUNCTION_TYPE, provider);
	}
}