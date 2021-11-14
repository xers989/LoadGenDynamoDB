package com.mongodb.dynamo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.mongodb.dynamo.load.DeleteCustomer;
import com.mongodb.dynamo.load.InsertCustomer;
import com.mongodb.dynamo.load.UpdateCustomer;


public class LoadGenData {
	
	private String awsSecretKey = null;
	private String awsAccessKey = null;
	
	private String dynamoEndpoint = null;
	private String dynamoRegion = null;
	
	private String dynamodbTable = null;
	
	
	public LoadGenData(String awsSecretKey, String awsAccessKey, String dynamoEndpoint, String dynamoRegion, String dynamoTable)
	{
		super();
		this.awsSecretKey = awsSecretKey;
		this.awsAccessKey = awsAccessKey;
		this.dynamoEndpoint = dynamoEndpoint;
		this.dynamoRegion = dynamoRegion;
		this.dynamodbTable = dynamoTable;
	}
	
	public void commandRunning(String dbcommand, String prefix, int startX, int endX, int sleepmi)
	{
		InsertCustomer insert = null;
		UpdateCustomer update = null;
		DeleteCustomer delete = null;
		
		if (dbcommand.equals("I"))
		{
    		insert = new InsertCustomer(awsSecretKey,awsAccessKey,dynamoEndpoint, dynamoRegion, dynamodbTable);
    		insert.generateCustomerData(prefix, startX, endX, sleepmi);
		}
		else if(dbcommand.equals("U"))
		{
			System.out.println ("Update"); 
    		update = new UpdateCustomer(awsSecretKey,awsAccessKey,dynamoEndpoint, dynamoRegion, dynamodbTable);
    		update.genUpdateCustomerData(prefix, startX, endX, sleepmi);
		}
		else if (dbcommand.equals("D"))
		{
			delete = new DeleteCustomer(awsSecretKey,awsAccessKey,dynamoEndpoint, dynamoRegion, dynamodbTable);
    		delete.genDeleteCustomerData(prefix, startX, endX, sleepmi);
		}
		else
		{
			System.out.println ("The operation command have to be one in the options [I, U, D]");
    	}
		
	}

    public static void main(String[] args) throws Exception {
    	String dbcommand = null;
    	String prefix = null;
    	int startX = 0;
    	int endX = 0;
    	
    	String awsSecretKey = null;
    	String awsAccessKey = null;
    	
    	String dynamoEndpoint = null;
    	String dynamoRegion = null; 
    	
    	String dynamodbTable = null;
    	
    	int sleepMiSec=0;
    	
    	LoadGenData load = null;
    	
    	
    	try {
    		
    		InputStream inputStream = new FileInputStream(args[0]);
            Properties prop = new Properties();
            
         // load a properties file
        	prop.load(inputStream);
        	
        	// get value by key
        	dbcommand = prop.getProperty("dynamodb.command");
        	prefix = prop.getProperty("dynamodb.prefix");
        	startX = (new Integer(prop.getProperty("dynamodb.start"))).intValue();
        	endX = (new Integer(prop.getProperty("dynamodb.end"))).intValue();
        	awsSecretKey = prop.getProperty("dynamodb.awsSecretKey");
        	awsAccessKey = prop.getProperty("dynamodb.awsAccessKey");

        	dynamoEndpoint = prop.getProperty("dynamodb.endpoint");
        	dynamoRegion = prop.getProperty("dynamodb.region");
        	
        	dynamodbTable = prop.getProperty("dynamodb.table");
        	sleepMiSec = (new Integer(prop.getProperty("sleepmi"))).intValue();
        	
        	
        	
        	load = new LoadGenData(awsSecretKey,awsAccessKey,dynamoEndpoint,dynamoRegion, dynamodbTable);
        	load.commandRunning(dbcommand, prefix, startX, endX, sleepMiSec);
        } catch (NumberFormatException ex) {
        	System.out.println("Start and End must be number format");
        	System.out.println(ex.toString());
        }
    	catch (IOException io) {
            io.printStackTrace();
        }
    	
    	

    }
}