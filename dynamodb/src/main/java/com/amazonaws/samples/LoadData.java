package com.amazonaws.samples;

import java.io.File;
import java.util.Iterator;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LoadData {

    public static void main(String[] args) throws Exception {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://dynamodb.us-west-2.amazonaws.com", "us-west-2"))
            .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("Rating");

        JsonParser parser = new JsonFactory().createParser(new File("rating.json"));

        JsonNode rootNode = new ObjectMapper().readTree(parser);
        Iterator<JsonNode> iter = rootNode.iterator();

        ObjectNode currentNode;

        while (iter.hasNext()) {
            currentNode = (ObjectNode) iter.next();

            int userId = currentNode.path("userId").asInt();
            int movieId = currentNode.path("movieId").asInt();

            try {
                table.putItem(new Item().withPrimaryKey("userId", userId, "movieId", movieId).withJSON("rating",
                    currentNode.path("rating").toString()));
                System.out.println("PutItem succeeded: " + userId + " " + movieId);

            }
            catch (Exception e) {
                System.err.println("Unable to add movie: " + userId + " " + movieId);
                System.err.println(e.getMessage());
                break;
            }
        }
        parser.close();
    }
}
