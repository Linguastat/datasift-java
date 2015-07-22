package com.linguastat.client;

import java.io.File;
import java.io.IOException;

import com.datasift.client.DataSiftClient;
import com.datasift.client.DataSiftConfig;
import com.datasift.client.core.Stream;
import com.datasift.client.stream.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class DataSiftOutputController
{   
	// json mapper utlility and json array object
	ObjectMapper mapper = new ObjectMapper();
	ArrayNode rootNode = null;
	// file prefix, max interactions per file and files created counter
	String fileNamePrefix = null;
	int maxInteractionsPerFile;
	int filesIndex = 1;
	
	
	public DataSiftOutputController(String fileNamePrefix, int maxInteractionsPerFile){
		this.fileNamePrefix = fileNamePrefix;
		this.maxInteractionsPerFile = maxInteractionsPerFile;
		this.rootNode = mapper.createArrayNode();
	}
	
	public void appendInteraction(JsonNode appendNode){
		this.rootNode.add(appendNode);
		
		// write to file
		if (rootNode.size() == maxInteractionsPerFile){
			writeToFile();
		}
	}

	public void writeToFile() {
		try {
			mapper.writeValue(new File("output", fileNamePrefix + filesIndex +".json"), rootNode);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// reinitialize the array that was already written
		this.rootNode = mapper.createArrayNode();
		// increment the file index
		this.filesIndex++;
	}

}