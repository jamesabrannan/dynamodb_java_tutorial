package com.tutorial.aws.dynamodb.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.tutorial.aws.dynamodb.model.Observation;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@Service
public class ObservationService {

	@Value("${cloud.aws.credentials.accessKey}")
	private String key;

	@Value("${cloud.aws.credentials.secretKey}")
	private String secretKey;

	private DynamoDbClient dynamoDbClient;

	@PostConstruct
	public void initialize() {
		AwsBasicCredentials awsCreds = AwsBasicCredentials.create(key, secretKey);
		DynamoDbClient client = DynamoDbClient.builder().credentialsProvider(StaticCredentialsProvider.create(awsCreds))
				.region(Region.US_EAST_1).build();
		this.dynamoDbClient = client;
	}

	@PreDestroy
	public void preDestroy() {
		this.dynamoDbClient.close();
	}
	
	
	

	

	public List<Observation> getObservationsForStation(String stationId){

		ArrayList<Observation> observations = new ArrayList<>();
		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("stationid", AttributeValue.builder().n(stationId).build());
	
		Condition condition = Condition.builder().comparisonOperator(ComparisonOperator.EQ)
				.attributeValueList(AttributeValue.builder().n(stationId).build()).build();
		
		Map<String, Condition> conditions = new HashMap<String, Condition>();
		conditions.put("stationid",condition);
		
		QueryRequest request = QueryRequest.builder().tableName("Observation").indexName("stationid-index")
				.keyConditions(conditions).build();
		
		List<Map<String, AttributeValue>> results = this.dynamoDbClient.query(request).items();
		
		for(Map<String,AttributeValue> responseItem: results) {
			
			Observation observation = new Observation();
			observation.setDate(responseItem.get("date").s());
			observation.setTime(responseItem.get("time").s());
			observation.setImage(responseItem.get("image").b().asUtf8String());
			observation.setStationid(Long.parseLong(responseItem.get("stationid").n()));
			
			if(responseItem.get("tags") != null && responseItem.get("tags").ss().size() > 0)
			{
				HashSet<String> vals = new HashSet<>();
				responseItem.get("tags").ss().stream().forEach(x->vals.add(x));
				observation.setTags(vals);
			}
			
			observations.add(observation);
			
		}
		
		return observations;
		
	}
	
	

	
	
	public Observation getObservation(String observationId) {
		HashMap<String,AttributeValue> key = new HashMap<String,AttributeValue>();
		key.put("id", AttributeValue.builder().s(observationId).build());
		GetItemRequest request = GetItemRequest.builder().tableName("Observation").key(key).build();
		Map<String,AttributeValue> responseItem = this.dynamoDbClient.getItem(request).item();
		
		Observation observation = new Observation();
		observation.setDate(responseItem.get("date").s());
		observation.setTime(responseItem.get("time").s());
		observation.setImage(responseItem.get("image").b().asUtf8String());
		observation.setStationid(Long.parseLong(responseItem.get("stationid").n()));
		
		if(responseItem.get("tags") != null && responseItem.get("tags").ss().size() > 0)
		{
			HashSet<String> vals = new HashSet<>();
			responseItem.get("tags").ss().stream().forEach(x->vals.add(x));
			observation.setTags(vals);
		}
		
		return observation;
		
	}
	
	public void batchWriteObservations(List<Observation> observations) {
		
		ArrayList<WriteRequest> requests = new ArrayList<>();
		HashMap<String, AttributeValue> observationMap = new HashMap<>();
		
		for(Observation observation : observations) {
		
			observationMap.put("id",
					AttributeValue.builder().s(observation.getStationid() + observation.getDate() + observation.getTime()).build());
			observationMap.put("stationid", AttributeValue.builder().n(Long.toString(observation.getStationid())).build());
			observationMap.put("date", AttributeValue.builder().s(observation.getDate()).build());
			observationMap.put("time", AttributeValue.builder().s(observation.getTime()).build());
			observationMap.put("image", AttributeValue.builder().b(SdkBytes.fromUtf8String(observation.getImage())).build());
			if (observation.getTags() != null) {
				observationMap.put("tags", AttributeValue.builder().ss(observation.getTags()).build());
			}
			
			WriteRequest writeRequest = WriteRequest.builder().putRequest(PutRequest.builder().item(observationMap).build()).build();
			requests.add(writeRequest);
		}
		
		HashMap<String,List<WriteRequest>> batchRequests = new HashMap<>();
		batchRequests.put("Observation", requests);
		
		BatchWriteItemRequest request = BatchWriteItemRequest.builder().requestItems(batchRequests).build();
		this.dynamoDbClient.batchWriteItem(request);
		
	}
	
	public void updateObservationTags(List<String> tags, String observationId) {

		HashMap<String, AttributeValue> tagMap = new HashMap<String, AttributeValue>();
		tagMap.put(":tagval", AttributeValue.builder().ss(tags).build());

		HashMap<String, AttributeValue> key = new HashMap<>();
		key.put("id", AttributeValue.builder().s(observationId).build());

		UpdateItemRequest request = UpdateItemRequest.builder().tableName("Observation").key(key)
				.updateExpression("SET tags = :tagval").expressionAttributeValues(tagMap).build();

		
		this.dynamoDbClient.updateItem(request);

	}

	public void deleteObservation(String observationId) {
		HashMap<String, AttributeValue> key = new HashMap<>();
		key.put("id", AttributeValue.builder().s(observationId).build());
		DeleteItemRequest deleteRequest = DeleteItemRequest.builder().key(key).tableName("Observation").build();
		this.dynamoDbClient.deleteItem(deleteRequest);
	}

	public void writeObservation(Observation observation) {

		HashMap<String, AttributeValue> observationMap = new HashMap<String, AttributeValue>();
		observationMap.put("id",
				AttributeValue.builder().s(observation.getStationid() + observation.getDate() + observation.getTime()).build());
		observationMap.put("stationid", AttributeValue.builder().n(Long.toString(observation.getStationid())).build());
		observationMap.put("date", AttributeValue.builder().s(observation.getDate()).build());
		observationMap.put("time", AttributeValue.builder().s(observation.getTime()).build());
		observationMap.put("image", AttributeValue.builder().b(SdkBytes.fromUtf8String(observation.getImage())).build());
		if (observation.getTags() != null) {
			observationMap.put("tags", AttributeValue.builder().ss(observation.getTags()).build());
		}

		PutItemRequest request = PutItemRequest.builder().tableName("Observation").item(observationMap).build();
		this.dynamoDbClient.putItem(request);
		
	}

}
