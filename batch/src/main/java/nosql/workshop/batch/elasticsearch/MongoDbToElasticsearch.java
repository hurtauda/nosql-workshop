package nosql.workshop.batch.elasticsearch;

import com.mongodb.*;
import nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.UnknownHostException;

import static nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Transferts les documents depuis MongoDB vers Elasticsearch.
 */
public class MongoDbToElasticsearch {

    public static void main(String[] args) throws UnknownHostException {

        MongoClient mongoClient = null;

        long startTime = System.currentTimeMillis();
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "PSG").build();
        try (Client elasticSearchClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(ES_DEFAULT_HOST, ES_DEFAULT_PORT))){
            checkIndexExists("installations", elasticSearchClient);

            mongoClient = new MongoClient();

            // cursor all database objects from mongo db
            DBCursor cursor = ElasticSearchBatchUtils.getMongoCursorToAllInstallations(mongoClient);

            BulkRequestBuilder bulkRequest = elasticSearchClient.prepareBulk();

            while (cursor.hasNext()) {
                DBObject object = cursor.next();

                String objectId = (String) object.get("_id");
                object.removeField("dateMiseAJourFiche");

                bulkRequest.add(elasticSearchClient.prepareIndex("installations", "installation", "1")
                        .setSource(jsonBuilder()
                                .startObject()
                                .field(objectId, object)
                                .endObject()
                        )
                );
            }
            BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();

            dealWithFailures(bulkItemResponses);

            System.out.println("Inserted all documents in " + (System.currentTimeMillis() - startTime) + " ms");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }
}
