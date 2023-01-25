/*
 * Copyright 2022
 * Listware
 */

package org.listware.core.cmdb;

import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.listware.core.documents.LinkDocument;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.functions.link.LinkTrigger;
import org.listware.core.provider.functions.object.ObjectTrigger;
import org.listware.core.utils.exceptions.AlreadyLinkException;
import org.listware.core.utils.exceptions.NoLinkException;
import org.listware.core.utils.exceptions.UnknownIdException;
import org.listware.io.grpc.FinderClient;
import org.listware.io.grpc.QDSLClient;
import org.listware.io.utils.TypedValueDeserializer;
import org.listware.sdk.Functions;
import org.listware.sdk.pbcmdb.Core;
import org.listware.sdk.pbcmdb.pbfinder.Finder;
import org.listware.sdk.pbcmdb.pbqdsl.QDSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class Cmdb {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Cmdb.class);

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
		public static final String SYSTEM = "system";
	}

	private ObjectClient objectClient = new ObjectClient();
	public LinkClient linkClient = new LinkClient();
	public QDSLClient qdslClient = new QDSLClient();
	public FinderClient finderClient = new FinderClient();

	public void shutdown() throws InterruptedException {
		objectClient.shutdown();
		linkClient.shutdown();
		qdslClient.shutdown();
		finderClient.shutdown();
	}

	// R same for types/objects/system
	public ObjectDocument readDocument(String id) throws Exception {
		return objectClient.readDocument(id);
	}

	private ObjectDocument updateDocument(ObjectDocument document) throws Exception {
		document = objectClient.updateDocument(document.getId(), document.serialize());
		LOG.info("updated " + document.getId());
		return document;
	}

	// D same for types/objects/system
	private void removeDocument(ObjectDocument document) throws Exception {
		objectClient.removeDocument(document.getId());
		LOG.info("deleted " + document.getId());
	}

	// R links
	public LinkDocument readLinkDocument(String id) throws Exception {
		return linkClient.readDocument(id);
	}

	public LinkDocument readLinkDocument(String from, String name) throws Exception {
		Finder.Response response = finderClient.from(from, name);
		if (response.getLinksCount() == 0) {
			throw new NoLinkException(from, name);
		}
		return LinkDocument.deserialize(response.getLinks(0).getPayload());
	}

	// do not duplicate link with name
	public void checkFrom(ObjectDocument parent, String name) throws Exception {
		Finder.Response response = finderClient.from(parent.getId(), name);
		if (response.getLinksCount() > 0) {
			throw new AlreadyLinkException(parent.getId(), name);
		}
	}

	public LinkDocument updateLinkDocument(LinkDocument document) throws Exception {
		document = linkClient.updateDocument(document.getId(), document.serialize());
		LOG.info("updated " + document.getId());
		return document;
	}

	// D links
	public void removeDocument(LinkDocument document) throws Exception {
		linkClient.removeDocument(document.getId());
		LOG.info("deleted " + document.getId());
	}

	/*******************************************************************************************/
	// C for SYSTEM
	public ObjectDocument createSystem(ObjectDocument document) throws Exception {
		document = objectClient.createDocument(Collections.SYSTEM, document.serialize());

		LOG.info("created system " + document.getId());

		return document;
	}

	public ObjectDocument createSystem(ObjectDocument parent, ObjectDocument document) throws Exception {
		document = objectClient.createDocument(Collections.SYSTEM, document.serialize());

		LOG.info("created system " + document.getId());

		// link root -> object
		createLink(parent, document, LinkTypes.SYSTEM, document.getKey());

		return document;
	}

	/*******************************************************************************************/

	// types C
	public ObjectDocument createType(ObjectDocument document) throws Exception {
		document = objectClient.createDocument(Collections.TYPES, document.serialize());

		ObjectDocument types = readDocument("system/types");

		// link from types -> type
		createLink(types, document, LinkTypes.TYPE, document.getKey());

		LOG.info("created type " + document.getId());

		return document;
	}

	public ObjectDocument updateType(Context context, ObjectDocument document) throws Exception {
		document = updateDocument(document);

		return document;
	}

	public void removeType(Context context, ObjectDocument document) throws Exception {
		removeDocument(document);
	}

	/*******************************************************************************************/

	// only from type
	public ObjectDocument createObject(ObjectDocument type, ObjectDocument document) throws Exception {
		ObjectDocument objects = readDocument("system/objects");

		document = objectClient.createDocument(Collections.OBJECTS, document.serialize());

		// link type -> object ($uuid)
		createLink(type, document, type.getKey(), document.getKey());

		// link objects -> object ($uuid)
		createLink(objects, document, type.getKey(), document.getKey());

		LOG.info("created object " + document.getId());

		return document;
	}

	public ObjectDocument createObject(Context context, ObjectDocument type, ObjectDocument document) throws Exception {
		document = createObject(type, document);

		Functions.FunctionContext pbFunctionContext = ObjectTrigger.Trigger(document.getId(), Core.Method.CREATE);

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

		context.send(ObjectTrigger.FUNCTION_TYPE, document.getId(), typedValue);

		return document;
	}

	// with parent
	public ObjectDocument createObject(ObjectDocument type, ObjectDocument parent, ObjectDocument document, String name)
			throws Exception {

		checkFrom(parent, name);

		document = createObject(type, document);

		// link parent -> object ($uuid)
		createLink(parent, document, type.getKey(), name);

		return document;
	}

	public ObjectDocument createObject(Context context, ObjectDocument type, ObjectDocument parent,
			ObjectDocument document, String name) throws Exception {

		checkFrom(parent, name);

		document = createObject(context, type, document);

		// link parent -> object ($uuid)
		createLink(parent, document, type.getKey(), name);

		return document;
	}

	public ObjectDocument updateObject(Context context, ObjectDocument document) throws Exception {
		document = updateDocument(document);
		Functions.FunctionContext pbFunctionContext = ObjectTrigger.Trigger(document.getId(), Core.Method.UPDATE);
		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);
		context.send(ObjectTrigger.FUNCTION_TYPE, document.getId(), typedValue);

		return document;
	}

	public void removeObject(Context context, ObjectDocument document) throws Exception {
		removeDocument(document);
		// move trigger to link type -> objects

//		Functions.FunctionContext pbFunctionContext = ObjectTrigger.Trigger(document.getId(), Core.Method.DELETE);
//
//		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);
//
//		context.send(ObjectTrigger.FUNCTION_TYPE, document.getId(), typedValue);
	}

	// Links

	public LinkDocument createLink(ObjectDocument parent, ObjectDocument document, String type, String name)
			throws Exception {

		checkFrom(parent, name);

		LinkDocument link = new LinkDocument(parent.getId(), document.getId(), type, name);

		link = linkClient.createDocument(Collections.LINKS, link.serialize());

		LOG.info("created link" + link.getId());

		return link;
	}

	public LinkDocument createLink(ObjectDocument parent, ObjectDocument document, String type, String name,
			ByteString payload) throws Exception {

		checkFrom(parent, name);

		LinkDocument link = new LinkDocument(parent.getId(), document.getId(), type, name);
		link.replaceProperties(payload);

		link = linkClient.createDocument(Collections.LINKS, link.serialize());

		LOG.info("created link" + link.getId());

		return link;
	}

	public LinkDocument createLink(Context context, ObjectDocument parent, ObjectDocument document, String type,
			String name, ByteString payload) throws Exception {

		LinkDocument link = createLink(parent, document, type, name, payload);

		Functions.FunctionContext pbFunctionContext = LinkTrigger.Trigger(link.getId(), Core.Method.CREATE);

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

		context.send(LinkTrigger.FUNCTION_TYPE, link.getId(), typedValue);

		LOG.info("created link" + link.getId());

		return link;
	}

	public LinkDocument updateLink(Context context, LinkDocument document) throws Exception {
		document = updateLinkDocument(document);

		Functions.FunctionContext pbFunctionContext = LinkTrigger.Trigger(document.getId(), Core.Method.UPDATE);

		TypedValue typedValue = TypedValueDeserializer.fromMessageLite(pbFunctionContext);

		context.send(LinkTrigger.FUNCTION_TYPE, document.getId(), typedValue);

		return document;
	}

	public void bootstrap() throws Exception {
		ObjectDocument root = null;
		try {
			root = readDocument("system/root");
		} catch (Exception ex) {
			LOG.error(ex.getLocalizedMessage());
			root = new ObjectDocument(SystemKeys.ROOT);
			root = createSystem(root);
		}

		ObjectDocument objects = null;
		try {
			objects = readDocument("system/objects");
		} catch (Exception ex) {
			LOG.error(ex.getLocalizedMessage());
			objects = new ObjectDocument(SystemKeys.OBJECTS);
			objects = createSystem(root, objects);
		}

		ObjectDocument types = null;
		try {
			types = readDocument("system/types");
		} catch (Exception ex) {
			LOG.error(ex.getLocalizedMessage());
			types = new ObjectDocument(SystemKeys.TYPES);
			types = createSystem(root, types);
		}

		ObjectDocument functionContainer = null;
		try {
			functionContainer = readDocument("types/function-container");
		} catch (Exception ex) {
			LOG.error(ex.getLocalizedMessage());
			functionContainer = new ObjectDocument("function-container");
			functionContainer = createType(functionContainer);
		}

		ObjectDocument function = null;
		try {
			function = readDocument("types/function");
		} catch (Exception ex) {
			LOG.error(ex.getLocalizedMessage());
			function = new ObjectDocument("function");
			function = createType(function);
		}

		QDSL.Options options = QDSL.Options.newBuilder().build();

		QDSL.Elements elements = qdslClient.qdsl("functions.root", options);
		ObjectDocument functions = null;
		if (elements.getElementsCount() == 0) {
			functions = new ObjectDocument();
			functions = createObject(function, root, functions, "functions");
		} else {
			functions = readDocument(elements.getElements(0).getId());
		}

		elements = qdslClient.qdsl("system.functions.root", options);
		ObjectDocument system = null;
		if (elements.getElementsCount() == 0) {
			system = new ObjectDocument();
			system = createObject(functionContainer, functions, system, "system");
		} else {
			system = readDocument(elements.getElements(0).getId());
		}

		elements = qdslClient.qdsl("types.system.functions.root", options);
		ObjectDocument typesFunction = null;
		if (elements.getElementsCount() == 0) {
			typesFunction = new ObjectDocument();
			typesFunction = createObject(function, system, typesFunction, "types");
		} else {
			typesFunction = readDocument(elements.getElements(0).getId());
		}

		elements = qdslClient.qdsl("objects.system.functions.root", options);
		ObjectDocument objectsFunction = null;
		if (elements.getElementsCount() == 0) {
			objectsFunction = new ObjectDocument();
			objectsFunction = createObject(function, system, objectsFunction, "objects");
		} else {
			objectsFunction = readDocument(elements.getElements(0).getId());
		}

		elements = qdslClient.qdsl("links.system.functions.root", options);
		ObjectDocument linksFunction = null;
		if (elements.getElementsCount() == 0) {
			linksFunction = new ObjectDocument();
			linksFunction = createObject(function, system, linksFunction, "links");
		} else {
			linksFunction = readDocument(elements.getElements(0).getId());
		}
	}

	public String getTypeId(String id) throws Exception {
		QDSL.Options options = QDSL.Options.newBuilder().setType(true).build();

		String query = String.format("*[?@._id == '%s'?].objects", id);

		QDSL.Elements elements = qdslClient.qdsl(query, options);

		for (QDSL.Element element : elements.getElementsList()) {
			return "types/" + element.getType();
		}
		throw new UnknownIdException(id);
	}

}
