package com.app.customindexer.elasticsearch.client;

import java.math.BigInteger;
import java.util.concurrent.Callable;

public class IndexingTask implements Callable<Boolean> {
	private long documentId;
	private BigInteger lowerRange;
	private BigInteger higherRange;

	@Override
	public Boolean call() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
