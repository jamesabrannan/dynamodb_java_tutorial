package com.tutorial.aws.dynamodb.api;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.tutorial.aws.dynamodb.model.Observation;
import com.tutorial.aws.dynamodb.service.ObservationService;




@RestController
@RequestMapping(value = "/observations")
public class ObservationApiController {

	@Autowired
	ObservationService observationService;
	
	@PostMapping("/observation")
	public void saveObservation(@RequestBody Observation observation) {
		this.observationService.writeObservation(observation);
	}
	
	@PostMapping("/observation/batch")
	public void batchSaveObservation(@RequestBody List<Observation> observations) {
		this.observationService.batchWriteObservations(observations);
	}
	
	@DeleteMapping("/observation/{observationid}/delete")
	public void deleteObservation(@PathVariable("observationid") String observationId) {
		this.observationService.deleteObservation(observationId);
	}
	
	@PostMapping("/observation/{observationid}/updatetags")
	public void updateObservationTags(@PathVariable("observationid") String observationId, @RequestBody List<String> tags) {
		this.observationService.updateObservationTags(tags, observationId);
	}
	
	@GetMapping("/observation/{observationid}")
	public Observation getObservation(@PathVariable("observationid") String observationId) {
		return this.observationService.getObservation(observationId);
	}
	
	@GetMapping("/station/{stationid}")
	public List<Observation> getObservations(@PathVariable("stationid") String stationId) {
		return this.observationService.getObservationsForStation(stationId);
	}
	
}
