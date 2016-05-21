package com.customdataindexr.elasticsearch.client.reindex.action;

import com.app.customindexer.elasticsearch.client.ElasticSearchClient;
import com.reindexing.elasticsearch.client.alias.action.AliasAction;

public class ReindexAction {
	private AliasAction aliasAction = new AliasAction();
	private ElasticSearchClient esClient;
	private ESBasedReindexAction esBasedReIndexAction;
	private ScrollBasedReIndexAction scrollBasedReIndexAction;

	public ReindexAction(ElasticSearchClient client) {
		this.esClient = client;
		esBasedReIndexAction = new ESBasedReindexAction(esClient);
		scrollBasedReIndexAction = new ScrollBasedReIndexAction(esClient);
	}

	public ReindexingResponse reindex(int reIndexingMethod,String oldIndex,String newIndex,boolean deleteOldIndex,String oldIndexAlias,boolean pointAliasToNewIndex, boolean rollbackChangesUponFailure) {
		ReIndexingRequest request = new ReIndexingRequest(oldIndex, newIndex);
		ReindexingResponse response = null;
		response.setStartTime(System.currentTimeMillis());
		try {
			switch(reIndexingMethod) {
			case 1:
				response = esBasedReIndexAction.reindex(request);
				break;
			case 2:
				response = scrollBasedReIndexAction.reindex(request);
				break;
			}
		}
		catch (Throwable failure){
			response = new ReindexingResponse();
			response.setFailure(failure);
			response.setHasFailures(true);
			if(rollbackChangesUponFailure) 
				return response;	
		}
		finally {
			response.setEndTime(System.currentTimeMillis());
		}
		if(pointAliasToNewIndex) {
			aliasAction.pointAliastoNewIndex(oldIndexAlias, newIndex, oldIndex);
		}
		if(deleteOldIndex) {
			esClient.getClient()
			.prepareDelete()
			.setIndex(oldIndex)
			.execute()
			.actionGet();
		}		
		return response;	
	}
}