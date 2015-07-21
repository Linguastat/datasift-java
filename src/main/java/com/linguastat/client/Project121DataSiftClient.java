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
    	ObjectMapper mapper = new ObjectMapper();
    	// file prefix and files created counter
    	String fileNamePrefix = null;
    	int maxInteractionsPerFile;
    	int filesCreated = 1;
    	// main Array container node
    	ArrayNode rootNode = mapper.createArrayNode();

        public Subscription(Stream stream, String fileNamePrefix, int maxInteractionsPerFile) {
            super(stream);
            this.fileNamePrefix = fileNamePrefix;
            this.maxInteractionsPerFile = maxInteractionsPerFile;
        }
     
        public void onDataSiftLogMessage(DataSiftMessage di) {
            System.out.println((di.isError() ? "Error" : di.isInfo() ? "Info" : "Warning") + ":\n" + di);
        }
     
        public void onMessage(Interaction i) {
            System.out.println("INTERACTION:\n" + i);
            // add the interaction
            rootNode.add(i.getData());
            
            if(rootNode.size() == maxInteractionsPerFile){
            	try {
					mapper.writeValue(new File("output", fileNamePrefix+filesCreated+".json"), rootNode);
					this.rootNode = mapper.createArrayNode();
					this.filesCreated++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
            
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
        public void exceptionCaught(Throwable t) {
            t.printStackTrace();
            // TODO: do something useful..!
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
     
        try {
            
            // Compile filter passed in the configuration
            Stream stream = datasift.compile(csdl).sync();
            
            datasift.liveStream().onError(new ErrorHandler()); // handles stream errors
            datasift.liveStream().onStreamEvent(new DeleteHandler()); // handles data deletes
             
            // Subscribe to the stream
            datasift.liveStream().subscribe(new Subscription(stream, fileNamePrefix, maxInteractionsPerFile));
            
            // 
        } 
        catch(Exception ex)
        {
            // TODO: Your exception handling here
        	System.out.println(ex);
        }
        

    }

}