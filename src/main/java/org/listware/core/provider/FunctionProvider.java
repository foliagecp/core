/* Copyright 2022 Listware */

package org.listware.core.provider;

import java.util.Arrays;
import java.util.Collection;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.listware.io.utils.Constants.Cmdb;
import org.listware.io.utils.QDSLClient;
import org.listware.sdk.pbcmdb.pbqdsl.QDSL;
import org.listware.core.documents.LinkDocument;
import org.listware.core.documents.ObjectDocument;
import org.listware.core.provider.functions.Link;
import org.listware.core.provider.functions.Log;
import org.listware.core.provider.functions.Object;
import org.listware.core.provider.functions.ObjectTrigger;
import org.listware.core.provider.functions.Register;
import org.listware.core.provider.functions.Router;
import org.listware.core.provider.functions.Type;
import org.listware.core.provider.functions.TypeTrigger;
import org.listware.core.provider.utils.Cmdb.Collections;
import org.listware.core.provider.utils.Cmdb.LinkTypes;
import org.listware.core.provider.utils.Cmdb.SystemKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoGraph;
import com.arangodb.DbName;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.KeyType;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.DocumentCreateOptions;

public class FunctionProvider implements StatefulFunctionProvider {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(FunctionProvider.class);

	// graph name
	private static final String SYSTEM_GRAPH = "system";

	// system function types
	private static final String SYSTEM_TYPE = "system";
	private static final String FUNCTION_CONTAINER_TYPE = "function-container";
	private static final String FUNCTION_TYPE = "function";
	private static final String FUNCTIONS_LINK_NAME = "functions";
	private static final String SYSTEM_LINK_NAME = "system";
	private static final String TYPES_FUNCTION_LINK_NAME = "types";
	private static final String OBJECTS_FUNCTION_LINK_NAME = "objects";
	private static final String LINKS_FUNCTION_LINK_NAME = "links";
	private static final String ROUTER_FUNCTION_LINK_NAME = "router";

	private static final DbName DB_NAME = DbName.of(Cmdb.DBNAME);

	private QDSLClient client = new QDSLClient();
	private Log log = new Log();
	private Register register = new Register();

	private ArangoGraph graph;

	private Object object;
	private ObjectTrigger objectTrigger;
	private Type type;
	private TypeTrigger typeTrigger;
	private Link link;
	private Router qdsl;

	private ArangoCollection system, objects, types, links;

	public FunctionProvider() {
		graph = bootstrap();

		qdsl = new Router(client);
		type = new Type(graph);
		typeTrigger = new TypeTrigger(graph);
		object = new Object(graph);
		objectTrigger = new ObjectTrigger(graph);
		link = new Link(graph);
	}

	@Override
	public StatefulFunction functionOfType(FunctionType functionType) {
		if (functionType.equals(Router.FUNCTION_TYPE)) {
			return qdsl;
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

		return null;
	}

	private ArangoGraph bootstrap() {
		// TODO db and collections bootstrap
		ArangoDB arango = new ArangoDB.Builder().host(Cmdb.ADDR, Cmdb.PORT)
				// .useSsl(true).sslContext(sslContext)
				.serializer(new ArangoJack()).user(Cmdb.USER).password(Cmdb.PASSWORD).build();

		ArangoDatabase db = arango.db(DB_NAME);
		if (!db.exists()) {
			db.create();
		}

		system = db.collection(Collections.SYSTEM);
		if (!system.exists()) {
			// TODO set 'isSystem' for system
			CollectionCreateOptions opts = new CollectionCreateOptions().isSystem(true);
			system.create(opts);
		}

		types = db.collection(Collections.TYPES);
		if (!types.exists()) {
			types.create();
		}

		objects = db.collection(Collections.OBJECTS);
		if (!objects.exists()) {
			// TODO set 'uuid' for objects
			CollectionCreateOptions opts = new CollectionCreateOptions().keyOptions(false, KeyType.uuid, null, null);
			objects.create(opts);
		}

		links = db.collection(Collections.LINKS);
		if (!links.exists()) {
			// TODO set 'edge' for links
			CollectionCreateOptions ops = new CollectionCreateOptions().type(CollectionType.EDGES);
			links.create(ops);
		}

		// TODO create 'root'
		if (!system.documentExists(SystemKeys.ROOT)) {
			system.insertDocument(new ObjectDocument(SystemKeys.ROOT));
		}

		ObjectDocument root = system.getDocument(SystemKeys.ROOT, ObjectDocument.class);

		// TODO create 'objects'
		if (!system.documentExists(SystemKeys.OBJECTS)) {
			insertSystem(root, SystemKeys.OBJECTS);
		}

		ObjectDocument objectsBaseDocument = system.getDocument(SystemKeys.OBJECTS, ObjectDocument.class);

		// TODO create 'types'
		if (!system.documentExists(SystemKeys.TYPES)) {
			insertSystem(root, SystemKeys.TYPES);
		}

		ObjectDocument typesBaseDocument = system.getDocument(SystemKeys.TYPES, ObjectDocument.class);

		// TODO create 'function-container'
		if (!types.documentExists(FUNCTION_CONTAINER_TYPE)) {
			insertType(typesBaseDocument, FUNCTION_CONTAINER_TYPE);
		}

		// TODO create 'function'
		if (!types.documentExists(FUNCTION_TYPE)) {
			insertType(typesBaseDocument, FUNCTION_TYPE);
		}

		QDSL.Options options = QDSL.Options.newBuilder().build();

		// TODO create 'functions.root' from type 'function-container'
		QDSL.Elements elements = client.qdsl("functions.root", options);
		if (elements.getElementsCount() == 0) {

			ObjectDocument typeBaseDocument = types.getDocument(FUNCTION_CONTAINER_TYPE, ObjectDocument.class);

			ObjectDocument functionsBaseDocument = insertObject(typeBaseDocument, objectsBaseDocument, root,
					FUNCTIONS_LINK_NAME);

			ObjectDocument systemBaseDocument = insertObject(typeBaseDocument, objectsBaseDocument,
					functionsBaseDocument, SYSTEM_LINK_NAME);

			typeBaseDocument = types.getDocument(FUNCTION_TYPE, ObjectDocument.class);
			insertObject(typeBaseDocument, objectsBaseDocument, systemBaseDocument, TYPES_FUNCTION_LINK_NAME);
			insertObject(typeBaseDocument, objectsBaseDocument, systemBaseDocument, OBJECTS_FUNCTION_LINK_NAME);
			insertObject(typeBaseDocument, objectsBaseDocument, systemBaseDocument, LINKS_FUNCTION_LINK_NAME);
			insertObject(typeBaseDocument, objectsBaseDocument, systemBaseDocument, ROUTER_FUNCTION_LINK_NAME);
		}

		// TODO graph bootstrap
		ArangoGraph graph = db.graph(SYSTEM_GRAPH);
		if (!graph.exists()) {
			// TODO one edge of 1 links collection
			EdgeDefinition EdgeDefinition = new EdgeDefinition().collection(Collections.LINKS)
					.from(Collections.SYSTEM, Collections.TYPES, Collections.OBJECTS)
					.to(Collections.TYPES, Collections.OBJECTS);

			Collection<EdgeDefinition> edgeDefinitions = Arrays.asList(EdgeDefinition);
			db.createGraph(SYSTEM_GRAPH, edgeDefinitions);
		}
		return graph;
	}

	private ObjectDocument insertObject(ObjectDocument type, ObjectDocument object, ObjectDocument parent,
			String name) {
		// insert to 'objects'
		DocumentCreateEntity<ObjectDocument> documentCreateEntity = objects.insertDocument(new ObjectDocument(),
				new DocumentCreateOptions().returnNew(true));

		String id = documentCreateEntity.getId();
		String key = documentCreateEntity.getKey();

		// Link type -> object ($uuid)
		insertLink(type.getId(), id, key, type.getKey());

		// Link objects -> object ($uuid)
		insertLink(object.getId(), id, key, type.getKey());

		// Link parent -> object ('name')
		insertLink(parent.getId(), id, name, type.getKey());

		return documentCreateEntity.getNew();
	}

	private void insertType(ObjectDocument typesBaseDocument, String key) {
		DocumentCreateEntity<ObjectDocument> documentCreateEntity = types.insertDocument(new ObjectDocument(key));
		insertLink(typesBaseDocument.getId(), documentCreateEntity.getId(), key, LinkTypes.TYPE);
	}

	private void insertSystem(ObjectDocument root, String key) {
		DocumentCreateEntity<ObjectDocument> documentCreateEntity = system.insertDocument(new ObjectDocument(key));
		insertLink(root.getId(), documentCreateEntity.getId(), key, SYSTEM_TYPE);
	}

	private void insertLink(String from, String to, String name, String type) {
		LinkDocument baseEdgeDocument = new LinkDocument(from, to, name, type);
		links.insertDocument(baseEdgeDocument);
	}

	SSLContext createSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
		final SSLContext sslContext = SSLContext.getInstance("TLS");

		sslContext.init(null, new TrustManager[] { new X509TrustManager() {
			@Override
			public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
			}

			@Override
			public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
			}

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return new X509Certificate[0];
			}
		} }, null);

		HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
		HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {

			public boolean verify(String hostname, SSLSession session) {
				return true;
			}
		});
		return sslContext;
	}

}
