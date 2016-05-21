/**
 * 
 */
package com.reindexing.elasticsearch.client.alias.action;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;

import com.app.customindexer.elasticsearch.client.ElasticSearchClient;

/**
 * Interface for all operations related to alias.
 * 
 * <p>
 * What is the use of alias?
 * Alias must be used as it highly helps during reindex operation. Aliases can be switched in between indexes and deletion or addition of alias is atomic operation.
 * Alias removes tight coupling of application with the index name.
 * Indexes can be removed at any time still application can continue running without any problem.
 * There should be read_alias and write_alias.
 * read_aliases allow to read data from different index where write_alias allows to write data to only ond index. 
 *  
 * @author nikhil.bhide
 * @version 1.0
 */
public class AliasAction {
	private ElasticSearchClient esClient;
	/**
	 * Add alias to index
	 * @param aliasName The name of alias
	 * @param indexName The name of index to which alias to be added
	 * @return
	 */
	public IndicesAliasesResponse addAliastoIndex(String aliasName, String indexName) {
		return esClient.getClient().admin().indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet();
	}

	/**
	 * Remove alias from index
	 * @param aliasName The name of alias
	 * @param indexName The name of index from which alias to be removed
	 * @return
	 */
	public IndicesAliasesResponse removeAliasFromIndex(String aliasName, String indexName){
		return esClient.getClient().admin().indices().prepareAliases().removeAlias(indexName, aliasName).execute().actionGet();
	}

	/**
	 * Remove alias from index
	 * @param aliasName The name of alias
	 * @param newIndex The name of new index to which alias to be attached
	 * @param oldIndex The name of old index from which alias to be detached
	 * @return
	 */
	public IndicesAliasesResponse pointAliastoNewIndex(String aliasName,String newIndex, String oldIndex){
		return esClient.getClient().admin().indices().prepareAliases().addAlias(newIndex, aliasName).removeAlias(oldIndex, aliasName).execute().actionGet();
	}
}