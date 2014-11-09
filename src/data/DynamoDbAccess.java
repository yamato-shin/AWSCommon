package data;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

public class DynamoDbAccess {
	
	/**
	 * get Amazon DynamoDB client
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static AmazonDynamoDBClient getClient() throws FileNotFoundException, IOException {
			AWSCredentials credencials = new PropertiesCredentials(
			//		new FileInputStream("AwsCredentials.properties")
					DynamoDbAccess.class.getClassLoader().getResourceAsStream("AwsCredentials.properties")
				);
		AmazonDynamoDBClient client = new AmazonDynamoDBClient(credencials);
		client.setEndpoint("dynamodb.us-west-2.amazonaws.com");
		
		return client;
	}
}
