package com.customdataindexr.elasticsearch.client.reindex.action;

import java.io.IOException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import com.app.customindexer.elasticsearch.client.ElasticSearchClient;
import com.customdataindexr.elasticsearch.client.reindex.json.entities.Dest;
import com.customdataindexr.elasticsearch.client.reindex.json.entities.ReIndexRequest;
import com.customdataindexr.elasticsearch.client.reindex.json.entities.Source;
import com.customdataindexr.services.http.HttpClient;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class provides re-indexing capability using _reindex api.
 * 
 * <p>
 * Since 2.0, ES has been providing _reindex api. It is much efficient than scroll based api. 
 * This should be used for reindexing the index.
 * It provides lot of features such as cancelling reindexing task, overrride document (source/destination version), control conflicts etc..
 * 
 * @author nikhil.bhide
 * @version 1.0
 *
 */
public class ESBasedReindexAction {
	private ElasticSearchClient esClient;
	public ESBasedReindexAction(ElasticSearchClient client) {
		this.esClient = client;		
	}
	
	/**
	 * Reindexes old index to new index using reindex api.
	 *
	 * @param ReIndexingRequest 
	 * @throws IOException 
	 * @throws ClientProtocolException 
	 */
	public ReindexingResponse reindex(ReIndexingRequest reIndexingRequestrequest) throws ClientProtocolException, IOException {
		ReIndexRequest request = new ReIndexRequest();
		Source source = new Source();
		source.setIndex(reIndexingRequestrequest.getOldIndex());
		Dest destination = new Dest();
		destination.setIndex(reIndexingRequestrequest.getNewIndex());
		request.setDest(destination);
		request.setSource(source);
		String json = new ObjectMapper().writeValueAsString(request);
		StringEntity entity = new StringEntity(json);
		HttpClient httpClient = new HttpClient();
		ReindexingResponse response = (ReindexingResponse) httpClient.post(esClient.getESMasterNodeUrl().concat("/_reindex"), entity, true);
		ReindexingResponse reIndexingResponse = new ReindexingResponse();
		reIndexingResponse.setHasFailures(false);
		return reIndexingResponse;
	}
}