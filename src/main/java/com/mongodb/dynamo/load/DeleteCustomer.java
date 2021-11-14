package com.mongodb.dynamo.load;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class DeleteCustomer {
	
	private AmazonDynamoDB client = null;
	private DynamoDB dynamoDB = null;
	private Table table = null;
	
	private String awsSecretKey = null;
	private String awsAccessKey = null;
	private String dynamoEndpoint = null;
	private String dynamoRegion = null; 
	
	private String dynamodbTable = null;
	
	
	public DeleteCustomer(String awsSecretKey, String awsAccessKey, String dynamoEndpoint,String dynamoRegion, String dynamoDBTable)
	{
		super();
		this.awsSecretKey = awsSecretKey;
		this.awsAccessKey = awsAccessKey;
		this.dynamoEndpoint = dynamoEndpoint;
		this.dynamoRegion = dynamoRegion;
		this.dynamodbTable = dynamoDBTable;
	}


	
	private void makeConnection()
	{
		client = AmazonDynamoDBClientBuilder.standard()
	            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(dynamoEndpoint, dynamoRegion))
	            .withCredentials(new AWSStaticCredentialsProvider(
	                    new BasicAWSCredentials(awsAccessKey,awsSecretKey)))
	            .build();
		
		dynamoDB = new DynamoDB(client);
		table = dynamoDB.getTable(dynamodbTable);
	}
	
	private void deleteCustomerOne(String prefix, int customerId)
	{
		String strCustomerId = prefix+"-"+customerId;


//Size 300B

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
            .withPrimaryKey(new PrimaryKey("customerId", strCustomerId));

        // Conditional delete (we expect this to fail)

        try {
            System.out.println("Attempting a conditional delete...");
            table.deleteItem(deleteItemSpec);
            System.out.println("DeleteItem succeeded");
        }
        catch (Exception e) {
            System.err.println("Unable to delete item: " + strCustomerId);
            System.err.println(e.getMessage());
        }
	}
	
	public void genDeleteCustomerData(String prefix, int startX, int endX, int sleepmi)
	{
		makeConnection();
		
		try
		{
			
			for (int startIdx = startX; startIdx <= endX; startIdx++)
			{
				deleteCustomerOne(prefix, startIdx);
				Thread.sleep(sleepmi);
			}
		}catch (Exception e)
		{
			System.out.println("Load Gen Delete Error : Thread sleep error");
		}
	}
}