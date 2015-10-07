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
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ElasticSearchEventHandlerIntegrationTest {

    public static final String LABEL = "Brand";
    public static final String INDEX = "drug_info";
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

    private static enum RelType implements RelationshipType {
        TREATS
    }

    @After
    public void tearDown() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        client.shutdownClient();
        db.shutdown();
    }

    @Test
    public void testAfterCommit() throws Exception {
        // Create graph
        Transaction tx = db.beginTx();
        org.neo4j.graphdb.Node brand_1 = db.createNode(DynamicLabel.label(LABEL));
        org.neo4j.graphdb.Node brand_2 = db.createNode(DynamicLabel.label(LABEL));
        org.neo4j.graphdb.Node condition_1 = db.createNode(DynamicLabel.label("Condition"));
        org.neo4j.graphdb.Node condition_2 = db.createNode(DynamicLabel.label("Condition"));
        String brand_1_id = String.valueOf(brand_1.getId());
        String brand_2_id = String.valueOf(brand_2.getId());
        String condition_1_id = String.valueOf(condition_1.getId());
        String condition_2_id = String.valueOf(condition_2.getId());
        brand_1.setProperty("name", "tylenol");
        brand_2.setProperty("name", "coffee");
        condition_1.setProperty("name", "headache");
        condition_2.setProperty("name", "pain");
        org.neo4j.graphdb.Relationship treats_1 = brand_1.createRelationshipTo(condition_1, RelType.TREATS);
        org.neo4j.graphdb.Relationship treats_2 = brand_1.createRelationshipTo(condition_2, RelType.TREATS);
        org.neo4j.graphdb.Relationship treats_3 = brand_2.createRelationshipTo(condition_2, RelType.TREATS);
        tx.success();
        tx.close();
        
        Thread.sleep(1000); // wait for the async elasticsearch query to complete

        JestResult brand_1_response = client.execute(new Get.Builder(INDEX, brand_1_id).build());
        JestResult brand_2_response = client.execute(new Get.Builder(INDEX, brand_2_id).build());

        assertEquals(true, brand_1_response.isSucceeded());
        assertEquals(INDEX, brand_1_response.getValue("_index"));
        assertEquals(brand_1_id, brand_1_response.getValue("_id"));
        assertEquals(LABEL, brand_1_response.getValue("_type"));

        assertEquals(true, brand_2_response.isSucceeded());
        assertEquals(INDEX, brand_2_response.getValue("_index"));
        assertEquals(brand_2_id, brand_2_response.getValue("_id"));
        assertEquals(LABEL, brand_2_response.getValue("_type"));

        Map source_1 = brand_1_response.getSourceAsObject(Map.class);
        assertEquals(asList(LABEL), source_1.get("labels"));
        assertEquals(brand_1_id, source_1.get("id"));
        assertEquals("tylenol", source_1.get("name"));
        ArrayList<String> expectedCondition_1 = new ArrayList<>();
        expectedCondition_1.add("headache");
        expectedCondition_1.add("pain");
        assertEquals(expectedCondition_1, source_1.get("condition"));

        Map source_2 = brand_2_response.getSourceAsObject(Map.class);
        assertEquals(asList(LABEL), source_2.get("labels"));
        assertEquals(brand_2_id, source_2.get("id"));
        assertEquals("coffee", source_2.get("name"));
        ArrayList<String> expectedCondition_2 = new ArrayList<>();
        expectedCondition_2.add("pain");
        assertEquals(expectedCondition_2, source_2.get("condition"));
    }
}
