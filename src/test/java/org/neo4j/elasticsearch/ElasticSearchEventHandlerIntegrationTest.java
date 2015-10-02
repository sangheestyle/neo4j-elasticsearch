package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ElasticSearchEventHandlerIntegrationTest {

    public static final String LABEL = "dmetrics";
    public static final String INDEX = "test_index";
    public static final String INDEX_SPEC = INDEX + ":" + LABEL + "(name)";
    private GraphDatabaseService db;
    private JestClient client;

    @Before
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .build());
        client = factory.getObject();
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(config())
                .newGraphDatabase();

        // create index
        client.execute(new CreateIndex.Builder(INDEX).build());
    }

    private Map<String, String> config() {
        return stringMap(
                "elasticsearch.host_name", "http://localhost:9200",
                "elasticsearch.index_spec", INDEX_SPEC);
    }

    private static enum RelTypes implements RelationshipType {
        KNOWS
    }

    @After
    public void tearDown() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        client.shutdownClient();
        db.shutdown();
    }

    @Test
    public void testAfterCommit() throws Exception {
        Transaction tx = db.beginTx();
        org.neo4j.graphdb.Node node1 = db.createNode(DynamicLabel.label(LABEL));
        org.neo4j.graphdb.Node node2 = db.createNode(DynamicLabel.label(LABEL));
        org.neo4j.graphdb.Relationship relationship;
        String id1 = String.valueOf(node1.getId());
        String id2 = String.valueOf(node2.getId());
        node1.setProperty("name", "paul");
        node2.setProperty("name", "sanghee");
        relationship = node1.createRelationshipTo(node2, RelTypes.KNOWS);
        relationship.setProperty("where", "dmetrics");
        String relationship_id = String.valueOf(relationship.getId());
        System.out.println("rel" + relationship_id);
        tx.success();
        tx.close();
        
        Thread.sleep(1000); // wait for the async elasticsearch query to complete

        JestResult response1 = client.execute(new Get.Builder(INDEX, id1).build());
        JestResult response2 = client.execute(new Get.Builder(INDEX, id2).build());
        JestResult response3 = client.execute(new Get.Builder("relationships", relationship_id).build());

        assertEquals(true, response1.isSucceeded());
        assertEquals(INDEX, response1.getValue("_index"));
        assertEquals(id1, response1.getValue("_id"));
        assertEquals(LABEL, response1.getValue("_type"));

        assertEquals(true, response2.isSucceeded());
        assertEquals(INDEX, response2.getValue("_index"));
        assertEquals(id2, response2.getValue("_id"));
        assertEquals(LABEL, response2.getValue("_type"));

        assertEquals(true, response3.isSucceeded());
        assertEquals("relationships", response3.getValue("_index"));
        assertEquals(relationship_id, response3.getValue("_id"));
        assertEquals("relationship", response3.getValue("_type"));

        Map source1 = response1.getSourceAsObject(Map.class);
        assertEquals(asList(LABEL), source1.get("labels"));
        assertEquals(id1, source1.get("id"));
        assertEquals("paul", source1.get("name"));

        Map source2 = response2.getSourceAsObject(Map.class);
        assertEquals(asList(LABEL), source2.get("labels"));
        assertEquals(id2, source2.get("id"));
        assertEquals("sanghee", source2.get("name"));

        Map source3 = response3.getSourceAsObject(Map.class);
        assertEquals("relationship", source3.get("labels"));
        assertEquals(relationship_id, source3.get("id"));
        assertEquals("dmetrics", source3.get("where"));
    }
}
