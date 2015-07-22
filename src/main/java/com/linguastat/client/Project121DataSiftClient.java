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


public class Project121DataSiftClient
{    
	
	// Subscription handler
    public static class Subscription extends StreamSubscription {
    	DataSiftOutputController oc = null;

        public Subscription(Stream stream, DataSiftOutputController oc) {
            super(stream);
        	this.oc = oc;

        }
     
        public void onDataSiftLogMessage(DataSiftMessage di) {
            System.out.println((di.isError() ? "Error" : di.isInfo() ? "Info" : "Warning") + ":\n" + di);
        }
     
        public void onMessage(Interaction i) {
            System.out.println("INTERACTION:\n" + i);
            // add the interaction
            oc.appendInteraction(i.getData());
        }
    }
     
    // Delete handler
    public static class DeleteHandler extends StreamEventListener {
        public void onDelete(DeletedInteraction di) {
            // You must delete the interaction to stay compliant
            System.out.println("DELETED:\n " + di);
        }
    }
     
    // Error handler
    public static class ErrorHandler extends ErrorListener {
    	DataSiftOutputController oc = null;
    	
    	public ErrorHandler(DataSiftOutputController oc){
    		super();
    		this.oc = oc;
    	}
    	
        public void exceptionCaught(Throwable t) {
        	// write what is contained in the ArrayNode to file
    		oc.writeToFile();
        	// print the stack trace
            t.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        
		String userName = null;
		String apiKey = null;
		String csdl = null;
		// set defaults
		String fileNamePrefix = "interactions";
		int maxInteractionsPerFile = 3; 
		
		for (int i=0; i<args.length; i++) {
			String opt = args[i];
			if ("-username".equals(opt)) {
				userName = args[++i];
			}
			else if ("-apikey".equals(opt)) {
				apiKey = args[++i];
			}
			else if ("-csdl".equals(opt)) {
				csdl = args[++i];
			}
			else if ("-fileprefix".equals(opt)) {
				fileNamePrefix = args[++i];
			}
			else if ("-maxtweetsperfile".equals(opt)) {
				maxInteractionsPerFile = Integer.parseInt(args[++i]);
			}

		}
		
        // Now enter the username and API key and instantiate the client
        DataSiftConfig config = new DataSiftConfig(userName, apiKey);
        DataSiftClient datasift = new DataSiftClient(config);
        
        // now declare an output writer
     
        try {
            
            // Compile filter passed in the configuration
            Stream stream = datasift.compile(csdl).sync();
            
            // initialize the output controller
            DataSiftOutputController oc = new DataSiftOutputController(fileNamePrefix, maxInteractionsPerFile);
            
            datasift.liveStream().onError(new ErrorHandler(oc)); // handles stream errors
            datasift.liveStream().onStreamEvent(new DeleteHandler()); // handles data deletes
             
            // Subscribe to the stream
            datasift.liveStream().subscribe(new Subscription(stream, oc));
            
            // 
        } 
        catch(Exception ex)
        {
            // TODO: Your exception handling here
        	System.out.println(ex);
        }
        

    }

}