package com.example.demo;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.geometry.utils.Geohash;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class DemoElasticsearchApplication {
	
	public static void main(String[] args) throws IOException {
		SpringApplication.run(DemoElasticsearchApplication.class, args);
		
		RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
		
		// Single document APIs
		testSingleDocumentApi(client);
		
		
		testDeleteIndexRequest(client); // delete index
		
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
		testDeleteRequest(client);
		
		// TBD: Update API - update partial document
		
		// TBD: Term Vectors API
	}
	
	private static void testIndexRequest(RestHighLevelClient client) throws IOException {
		
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		{
			builder.field("category", "여성의류");
			builder.field("level", 3);
			builder.timeField("registered", new Date());
			builder.field("location", Geohash.stringEncode(126.88, 36.60)); //builder.field("location2", "36.60, 126.88");
		}
		builder.endObject();
		
		IndexRequest indexRequest = new IndexRequest("iteminfo2").id("v5KO4XMB_i31VgtFC0s1").source(builder);
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		log.info("response index=" + indexResponse.getIndex() + ", id=" + indexResponse.getId() + ", result=" + indexResponse.getResult());
	}
	
	private static void testDeleteIndexRequest(RestHighLevelClient client) throws IOException {
		DeleteIndexRequest request = new DeleteIndexRequest("iteminfo2");
		client.indices().delete(request, RequestOptions.DEFAULT);
	}
	
	private static void testGetRequest(RestHighLevelClient client) throws IOException {
		GetRequest request = new GetRequest("iteminfo2", "v5KO4XMB_i31VgtFC0s1");
		GetResponse response = client.get(request, RequestOptions.DEFAULT);
		
		if (response.isExists()) {
			log.info("found " + response.getIndex() + ", " + response.getId());
			if (response.isSourceEmpty()) {
				log.info("source is empty");
			} else {
				Map<String, Object> sourceMap = response.getSource();
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
		DeleteRequest request = new DeleteRequest("iteminfo2", "v5KO4XMB_i31VgtFC0s1");
		DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
		log.info("delete response.getResult " + response.getResult());
	}
	
	private static void testGetSourceRequest(RestHighLevelClient client) throws IOException {
		GetSourceRequest request = new GetSourceRequest("iteminfo2", "v5KO4XMB_i31VgtFC0s1");
		GetSourceResponse response = client.getSource(request, RequestOptions.DEFAULT);
		
		Map<String, Object> sourceMap = response.getSource();
		log.info("found source");
		log.info("category = " + sourceMap.get("category"));
		log.info("level = " + sourceMap.get("level"));
		String geohash = (String) sourceMap.get("location");
		log.info("location = " + Geohash.decodeLatitude(geohash) + ", " + Geohash.decodeLongitude(geohash));
	}
}
