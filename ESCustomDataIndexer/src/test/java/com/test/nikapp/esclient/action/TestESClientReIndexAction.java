package com.test.nikapp.esclient.action;

import org.junit.Test;
public class TestESClientReIndexAction {
	@Test
	public void reindex_oldIndexToNewIndex_Success(){
		
	}
	
	@Test(expected=RuntimeException.class)
	public void reindex_oldIndexToNewIndexDesitnationIndexNotCreated_RunTimeException() {
		
	}
	
	@Test
	public void reindex_oldIndexToNewIndexWithDateFilter_SuccessWithOnlyTwoDocuments(){
		
	}
}