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
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

public class InsertCustomer {
	
	private AmazonDynamoDB client = null;
	private DynamoDB dynamoDB = null;
	private Table table = null;
	
	private String awsSecretKey = null;
	private String awsAccessKey = null;
	
	private String dynamoEndpoint = null;
	private String dynamoRegion = null; 
	
	private String dynamodbTable = null;
	
	public InsertCustomer(String awsSecretKey, String awsAccessKey, String dynamoEndpoint,String dynamoRegion, String dynamoDBTable)
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
	
	private void insertCustomerOne(String prefix, int customerId)
	{
		String strCustomerId = prefix+"-"+customerId;

		String firstName = "Jon";
		String lastName  = "Doe";
		String email     = "jon.doe@email.com";
		    
//Size 300B
        final Map<String, Object> addressMap = new HashMap<String, Object>();
        addressMap.put("line1", "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ");
        addressMap.put("city", "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ");
        addressMap.put("country", "0123456789012345678901234567890123456789");
        addressMap.put("version", 1);
        
        try {
            System.out.println("Adding a new item..." + strCustomerId);
            if (table == null) makeConnection();
            else {            
	            PutItemOutcome outcome = table
	                .putItem(new Item().withPrimaryKey("customerId", strCustomerId).withString("firstName", firstName).withString("lastName", lastName).withString("email", email).withMap("address", addressMap));
	
	            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
            }

        }
        catch (Exception e) {
            System.err.println("Unable to add item: " + strCustomerId);
            System.err.println(e.getMessage());
        }
	}
	
	public void generateCustomerData(String prefix, int startX, int endX, int sleepmi)
	{
		makeConnection();
		System.out.println("Insert Data from "+startX + " to "+endX);
		try {
			for (int startIdx = startX; startIdx <= endX; startIdx++)
			{
				insertCustomerOne(prefix, startIdx);
				Thread.sleep(sleepmi);
			}
		}catch(Exception e)
		{
			System.out.println("Java Thread error : Thread.sleep"+e.toString());
		}
				
	}
}