/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import darkhipo.LazyLogger;

/**
 * Processes records retrieved from stock trades stream.
 *
 */
public class StockTradeRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(StockTradeRecordProcessor.class);
    private String kinesisShardId;

    // Reporting interval
    private static final long REPORTING_INTERVAL_MILLIS = 5000L; // 1 minute
    private long nextReportingTimeInMillis;

    // Checkpointing interval
    private static final long CHECKPOINT_INTERVAL_MILLIS = 5000L; // 1 minute
    private long nextCheckpointTimeInMillis;

    // Aggregates stats for stock trades
    private StockStats stockStats = new StockStats();
    private static final int TIME_WINDOW_MS = 2000;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
    	System.out.println("Process Records #" + records.size());
    	iteratorScan( checkpointer );
    	
    	//Kinesis is triggered by fresh incoming records, it triggers this method on the worker thread. 
    	//We instead of processing these records process records by scanning from last pointer.
    	/*
    	for (Record record : records) {
            // process record
            //StockTrade st = processRecord(record);
    		StockTrade st = StockTrade.fromJsonAsBytes(record.getData().array());
    		
            if ( st.getMyTime() + 3000 < System.currentTimeMillis() ) {
            	processRecord(record);
            	checkpoint(checkpointer, record);
            	System.out.println("YOUNG-pass: " + st);
            }
            else{
            	System.out.println("YOUNG IS TOO YOUNG: " + st);
            }
        }*/
        
        // If it is time to report stats as per the reporting interval, report stats
        if (System.currentTimeMillis() > nextReportingTimeInMillis) {
            reportStats();
            resetStats();
            nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        }

        // Checkpoint once every checkpoint interval
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            //checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    private void reportStats() {
    	System.out.println("****** Shard " + kinesisShardId + " stats for last 1 minute ******\n" +
                stockStats + "\n" +
                "****************************************************************\n");
    }

    private void resetStats() {
    	stockStats = new StockStats();
    }

    private StockTrade processRecord(Record record) {
    	StockTrade trade = StockTrade.fromJsonAsBytes(record.getData().array());
    	if (trade == null) {
    	    LOG.warn("Skipping record. Unable to parse record into StockTrade. Partition Key: " + record.getPartitionKey());
    	    return null;
    	}
    	String myStr = "";
    	myStr += " Seq-ID: " + record.getSequenceNumber();
    	myStr += " Part-K: " + record.getPartitionKey();
    	myStr += " Data: " + trade;
    	stockStats.addStockTrade(trade);
    	LazyLogger.log("kinesis-test-read.log", true, myStr);
    	return trade;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
        
    }
    private void checkpoint(IRecordProcessorCheckpointer checkpointer, Record record) {
        LOG.info( "Checkpointing Record-With-Seq-Num = " + record.getSequenceNumber() );
        try {
            checkpointer.checkpoint( record );
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

    private void iteratorScan( IRecordProcessorCheckpointer checkpointer ) {
    	AmazonKinesis kinesisClient = darkhipo.Utils.kinesisClient; 
        String shardId =  kinesisShardId;

        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(  "kinesis-test-stream" );
        getShardIteratorRequest.setShardId(shardId);

        if ( darkhipo.Utils.getLastSeqNum( ) != null ){
        	getShardIteratorRequest.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        	getShardIteratorRequest.setStartingSequenceNumber( darkhipo.Utils.getLastSeqNum( ) );
        }
        else{
        	getShardIteratorRequest.setShardIteratorType(ShardIteratorType.TRIM_HORIZON);
        	//System.out.println("EXITING");
        	//System.exit(1);
        }
        
        GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
        String shardIterator = getShardIteratorResult.getShardIterator();
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);
        //getRecordsRequest.setLimit( 10) ;
        
        GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
        List<Record> records = getRecordsResult.getRecords();
        
        if( !records.isEmpty() && records.size() > 0 ){
            Iterator<Record> iterator = records.iterator();
            while( iterator.hasNext() ) {
                Record record = iterator.next();
                byte[] bytes = record.getData().array();
                StockTrade trade = StockTrade.fromJsonAsBytes(record.getData().array());
                
                if ( trade.getMyTime() + TIME_WINDOW_MS < System.currentTimeMillis() ){
	                String recordData = new String(bytes); 
	                System.out.println("Iter-Pass-Shard Id : " + shardId + " Seq. No. is : "+ record.getSequenceNumber() +"  Record data :"+recordData);
	                	
	            	String myStr = " By-Iterator: ";
	            	myStr += " Seq-ID: " + record.getSequenceNumber();
	            	myStr += " Part-K: " + record.getPartitionKey();
	            	myStr += " Data: " + recordData;
	            	
	            	LazyLogger.log("kinesis-test-read.log", true, myStr);
	            	processRecord(record);
	            	darkhipo.Utils.writeLastSeqNum( record.getSequenceNumber() );
	            	
	            	//Kinesis Checkpointing disabled in favor of own.
	            	/*
	            	try {
	            		afterSeq = record.getSequenceNumber();
						checkpointer.checkpoint(record);
					} catch (KinesisClientLibDependencyException e) {
						e.printStackTrace();
					} catch (InvalidStateException e) {
						e.printStackTrace();
					} catch (ThrottlingException e) {
						e.printStackTrace();
					} catch (ShutdownException e) {
						e.printStackTrace();
					}
					*/
                }
                else{
                	System.out.println("MESSAGE IS TOO YOUNG: " + trade);
                }
            }
        }
    }
}