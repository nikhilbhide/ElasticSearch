package com.test.customdataindexr.esclient.action;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import org.apache.http.client.ClientProtocolException;
import org.junit.Test;

import com.app.customindexer.elasticsearch.client.ElasticSearchClient;
import com.customdataindexr.elasticsearch.client.reindex.action.ESBasedReindexAction;
import com.customdataindexr.elasticsearch.client.reindex.action.ReIndexingRequest;
import com.customdataindexr.elasticsearch.client.reindex.action.ReindexAction;
import com.customdataindexr.elasticsearch.client.reindex.action.ReindexingResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestESClientReIndexAction {
	@Test
	public void reindex_oldIndexToNewIndex_Success() throws UnknownHostException, InterruptedException, ExecutionException, JsonProcessingException {
		ElasticSearchClient client = new ElasticSearchClient();
		client.initClient("localhost", 9300, "http://localhost:9200");
		long startTime = System.currentTimeMillis();
		ReindexAction reIndexAction = new ReindexAction(client);
		HashMap<String, String> map = new HashMap();
		map.put("flight_no", "boeing001");
		map.put("flight_airline","AirIndia");
		ObjectMapper mapper = new ObjectMapper();
		String jsonActual = mapper.writeValueAsString(map);
		client.indexSingle("airportService","Flights",jsonActual,1);
	//	client.getClient().admin().indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet();
		//reIndexAction.reindex(2, "airportService", "airportService1", deleteOldIndex, oldIndexAlias, pointAliasToNewIndex, rollbackChangesUponFailure)
	}
	
	@Test(expected=RuntimeException.class)
	public void reindex_oldIndexToNewIndexDesitnationIndexNotCreated_RunTimeException() {
		
	}
	
	@Test
	public void reindex_oldIndexToNewIndexWithDateFilter_SuccessWithOnlyTwoDocuments(){
		
	}
	
	@Test
	public void esBasedreindex_existingIndexToNewIndex_Success() throws ClientProtocolException, IOException {
		ElasticSearchClient client = new ElasticSearchClient();
		client.initClient("localhost", 9300, "http://localhost:9200");
		ESBasedReindexAction reindexAction = new ESBasedReindexAction(client);
		ReIndexingRequest reIndexRequest = new ReIndexingRequest("twitter", "retwitter");
		ReindexingResponse response = reindexAction.reindex(reIndexRequest);
		assertTrue(!response.hasFailures());
	}
}