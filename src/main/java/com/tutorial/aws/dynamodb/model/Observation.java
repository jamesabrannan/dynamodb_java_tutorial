package com.tutorial.aws.dynamodb.model;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Observation {
	private long stationid;
	private String date;
	private String time;
	private String image;
  private Set<String> tags;
	
	public long getStationid() {
		return stationid;
	}


	public void setStationid(long stationid) {
		this.stationid = stationid;
	}


	public String getDate() {
		return date;
	}


	public void setDate(String date) {
		this.date = date;
	}


	public String getTime() {
		return time;
	}


	public void setTime(String time) {
		this.time = time;
	}


	public String getImage() {
		return image;
	}


	public void setImage(String image) {
		this.image = image;
	}

	public void setTags(Set<String> tags) {
		this.tags = tags;
	}
	
	public Set<String> getTags() {
		return this.tags;
	}
	

	@Override
	public String toString() {
		try {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return null;
		}
	}

}
