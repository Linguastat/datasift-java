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
	static ObjectMapper mapper = new ObjectMapper();
    // the root node to keep the json array
    static JsonNode rootNode = mapper.createArrayNode();
    // File to write
    static File outJson = new File("output/outjson.json");
	
	// Subscription handler
    public static class Subscription extends StreamSubscription {

        public Subscription(Stream stream) {
            super(stream);
        }
     
        public void onDataSiftLogMessage(DataSiftMessage di) {
            System.out.println((di.isError() ? "Error" : di.isInfo() ? "Info" : "Warning") + ":\n" + di);
        }
     
        public void onMessage(Interaction i) {
            System.out.println("INTERACTION:\n" + i);
            
            if (rootNode.size() < 1){
                ((ArrayNode)rootNode).add(i.getData());
            }
            else{
            	try {
					mapper.writeValue(outJson, rootNode);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            	System.exit(0);
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
        
        // TODO: Enter your username and API key
        DataSiftConfig config = new DataSiftConfig("user", "xxxxx");
        DataSiftClient datasift = new DataSiftClient(config);
     
        try {
            
            // Compile filter looking for mentions of brands
            String csdl = "interaction.content contains_any \"Samsung\"";
            Stream stream = datasift.compile(csdl).sync();
            
            datasift.liveStream().onError(new ErrorHandler()); // handles stream errors
            datasift.liveStream().onStreamEvent(new DeleteHandler()); // handles data deletes
             
            // Subscribe to the stream
            datasift.liveStream().subscribe(new Subscription(stream));
            
        } 
        catch(Exception ex)
        {
            // TODO: Your exception handling here
        	System.out.println(ex);
        }
        

    }

}