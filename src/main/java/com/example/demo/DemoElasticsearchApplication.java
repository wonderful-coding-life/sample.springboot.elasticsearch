package com.example.demo;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.extern.slf4j.Slf4j;


/*
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.8/java-rest-high.html
 */

@Slf4j
@SpringBootApplication
public class DemoElasticsearchApplication {
	
	private static final String INDEX_DEMO_ITEM = "demoitem";
	
	public static void main(String[] args) throws IOException {
		SpringApplication.run(DemoElasticsearchApplication.class, args);
		
		RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
		
		// Single document APIs
		testSingleDocumentApi(client);
		
		// Search APIs
		testSearchApi(client);
		
		//testDeleteIndexRequest(client); // delete index
		
		client.close();
	}
	
	private static void testSingleDocumentApi(RestHighLevelClient client) throws IOException {
		// Index API - create or update document
		testIndexRequest(client);
		
		// Get API - query document
		testGetRequest(client);
		
		// Get Source API
		testGetSourceRequest(client);
		
		// TBD: Exists API
		
		// Delete API - delete document
		// testDeleteRequest(client);
		
		// TBD: Update API - update partial document
		
		// TBD: Term Vectors API
	}

	private static void testSearchApi(RestHighLevelClient client) throws IOException {
		
		log.info("testing search API...");
		
		SearchRequest searchRequest = new SearchRequest("iteminfo"); 

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
		//sourceBuilder.query(QueryBuilders.termQuery("title", "아이폰")); 
		//sourceBuilder.query(QueryBuilders.matchQuery("title", "아이폰"));
		
//		sourceBuilder.query(QueryBuilders.boolQuery()
//                .must(QueryBuilders.termQuery("keywordField", "value"))
//                .mustNot(QueryBuilders.termQuery("keywordField2", "value2"))
//                .should(QueryBuilders.termQuery("keywordField3", "value3"))
//                .filter(QueryBuilders.termQuery("keywordField4", "value4")));

		
		sourceBuilder.query(QueryBuilders.multiMatchQuery("아이폰", "title", "description"));
		sourceBuilder.from(0); // paging from
		sourceBuilder.size(20); // paging count
		//sourceBuilder.storedField("_source.title");
		searchRequest.source(sourceBuilder);
		
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
		
		RestStatus status = searchResponse.status();
		log.info("testing search API... status=" + status);
		TimeValue took = searchResponse.getTook();
		log.info("testing search API... took=" + took);
		Boolean terminatedEarly = searchResponse.isTerminatedEarly();
		boolean timedOut = searchResponse.isTimedOut();
		int totalShards = searchResponse.getTotalShards();
		int successfulShards = searchResponse.getSuccessfulShards();
		int failedShards = searchResponse.getFailedShards();
		for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
		    // failures should be handled here
		}
		
		SearchHits hits = searchResponse.getHits();
		log.info("testing search API... SearchHits=" + hits);
		TotalHits totalHits = hits.getTotalHits();
		log.info("testing search API... TotalHits=" + totalHits);
		// the total number of hits, must be interpreted in the context of totalHits.relation
		long numHits = totalHits.value;
		// whether the number of hits is accurate (EQUAL_TO) or a lower bound of the total (GREATER_THAN_OR_EQUAL_TO)
		TotalHits.Relation relation = totalHits.relation;
		float maxScore = hits.getMaxScore();
		SearchHit[] searchHits = hits.getHits();
		
		log.info("testing searchHits = " + searchHits.length);
		
		for (SearchHit hit : searchHits) {
		    // do something with the SearchHit
			String index = hit.getIndex();
			String id = hit.getId();
			float score = hit.getScore();
			String sourceAsString = hit.getSourceAsString();
			
			//Map<String, DocumentField> fieldsAsMap = hit.getFields();
			//log.info(">>>" + fieldsAsMap.keySet());
			//log.info(">>> field " + hit.field("title").getValue());
			//log.info("title = " + fieldsAsMap.get("title").getValue());
			
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			String documentTitle = (String) sourceAsMap.get("title");
			String documentDescription = (String) sourceAsMap.get("description");
			log.info("score = " + score);
			log.info("title = " + documentTitle);
			log.info("description = " + documentDescription);
		}
	}
	
	private static void testIndexRequest(RestHighLevelClient client) throws IOException {
		
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		{
			builder.field("title", "아이폰 6S Plus");
			builder.field("description", "거의 새것이나 다름 없어요.");
			builder.field("category", "여성의류");
			builder.field("level", 3);
			builder.timeField("registered", new Date());
			builder.field("location", Geohash.stringEncode(126.88, 36.60)); //builder.field("location2", "36.60, 126.88");
		}
		builder.endObject();
		
		IndexRequest indexRequest = new IndexRequest(INDEX_DEMO_ITEM).id("v5KO4XMB_i31VgtFC0s1").source(builder);
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		log.info("response index=" + indexResponse.getIndex() + ", id=" + indexResponse.getId() + ", result=" + indexResponse.getResult());
	}
	
	private static void testDeleteIndexRequest(RestHighLevelClient client) throws IOException {
		DeleteIndexRequest request = new DeleteIndexRequest(INDEX_DEMO_ITEM);
		client.indices().delete(request, RequestOptions.DEFAULT);
	}
	
	private static void testGetRequest(RestHighLevelClient client) throws IOException {
		GetRequest request = new GetRequest(INDEX_DEMO_ITEM, "v5KO4XMB_i31VgtFC0s1");
		GetResponse response = client.get(request, RequestOptions.DEFAULT);
		
		if (response.isExists()) {
			log.info("found " + response.getIndex() + ", " + response.getId());
			if (response.isSourceEmpty()) {
				log.info("source is empty");
			} else {
				Map<String, Object> sourceMap = response.getSource();
				log.info("title = " + sourceMap.get("title"));
				log.info("description = " + sourceMap.get("description"));
				log.info("category = " + sourceMap.get("category"));
				log.info("level = " + sourceMap.get("level"));
				String geohash = (String) sourceMap.get("location");
				log.info("location = " + Geohash.decodeLatitude(geohash) + ", " + Geohash.decodeLongitude(geohash));
			}
		} else {
			log.info("not found " + response.getIndex() + ", " + response.getId());
		}
	}
	
	private static void testDeleteRequest(RestHighLevelClient client) throws IOException {
		DeleteRequest request = new DeleteRequest(INDEX_DEMO_ITEM, "v5KO4XMB_i31VgtFC0s1");
		DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
		log.info("delete response.getResult " + response.getResult());
	}
	
	private static void testGetSourceRequest(RestHighLevelClient client) throws IOException {
		GetSourceRequest request = new GetSourceRequest(INDEX_DEMO_ITEM, "v5KO4XMB_i31VgtFC0s1");
		GetSourceResponse response = client.getSource(request, RequestOptions.DEFAULT);
		
		Map<String, Object> sourceMap = response.getSource();
		log.info("found source");
		log.info("category = " + sourceMap.get("category"));
		log.info("level = " + sourceMap.get("level"));
		String geohash = (String) sourceMap.get("location");
		log.info("location = " + Geohash.decodeLatitude(geohash) + ", " + Geohash.decodeLongitude(geohash));
	}
}
