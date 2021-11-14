package com.mongodb.dynamo.load;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

public class UpdateCustomer {
	
	private AmazonDynamoDB client = null;
	private DynamoDB dynamoDB = null;
	private Table table = null;
		
	private String awsSecretKey = null;
	private String awsAccessKey = null;
	private String dynamoEndpoint = null;
	private String dynamoRegion = null; 
	private String dynamodbTable = null;
	
	
	
	public UpdateCustomer(String awsSecretKey, String awsAccessKey, String dynamoEndpoint,String dynamoRegion, String dynameDBTable)
	{
		super();
		this.awsSecretKey = awsSecretKey;
		this.awsAccessKey = awsAccessKey;
		this.dynamoEndpoint = dynamoEndpoint;
		this.dynamoRegion = dynamoRegion;
		this.dynamodbTable = dynameDBTable;
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
	
	private void updateCustomerOne(String prefix, int customerId)
	{
		String strCustomerId = prefix+"-"+customerId;
        
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("customerId", strCustomerId)
        		.withUpdateExpression("set address.version = address.version + :v, email = :e, address.userLanguage = :l")
                .withValueMap(new ValueMap().withNumber(":v", 1).withString(":e", "jon.doe@mongodb.com")
                    .withList(":l", Arrays.asList("Korean", "Enlish", "Chinese")))
                .withReturnValues(ReturnValue.UPDATED_NEW);

            try {
                System.out.println("Updating the item...");
                UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
                System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

            }
            catch (Exception e) {
                System.err.println("Unable to update customerId: " + strCustomerId);
                System.err.println(e.getMessage());
            }
	}
	
	public void genUpdateCustomerData(String prefix, int startX, int endX, int sleepmi)
	{
		makeConnection();
		try {
			for (int startIdx = startX; startIdx <= endX; startIdx++)
			{
				updateCustomerOne(prefix, startIdx);
				Thread.sleep(sleepmi);
			}
		}catch (Exception e)
		{
			System.out.println("Load Gen Update Error : Thread sleep error");
		}
				
	}
}