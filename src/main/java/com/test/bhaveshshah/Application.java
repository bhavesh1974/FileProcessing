package com.test.bhaveshshah;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Application {
	public static void main(String args[]) {
		//Check whether file is passed in the argument
		if (args.length < 1) {
			System.out.println("Please pass the file name as an argument.");
			return ;
		}
		
		//Check file exists in the folder
		File logFile = new File(args[0]);
		if (!logFile.exists()) {
			System.out.println(args[0]+ " does not exists.");
			return ;
		}
		
		Application application = new Application();
		application.processLogs(logFile);
	}
	
	//Process log file
	public void processLogs(File logFile) {
		//Initialize database
		Database db = new Database();
		try {
			db.initialize();
		} catch (SQLException e1) {
			e1.printStackTrace();
			return ;
		}
		
		Map<String, Event> events = new HashMap<>();
	    ObjectMapper mapper = new ObjectMapper();

		long record = 0;
	    //Read file through stream line by line (it will consume less memory)
	    try(LineIterator it = FileUtils.lineIterator(logFile, "UTF-8")) {
	      System.out.println("Start processing log file...");

	      while (it.hasNext()) {
	    	record++;
	        //Parse JSON to POJO
	        Event event = new ObjectMapper().readValue(it.nextLine() , Event.class);
	        
	        //Process Event
	        this.processEvent(events, event, db);
	        
	        if (record == 1 || record % 500 == 0) System.out.println("Log record " + record + " is being processed..."); 
	      }
	    } catch (IOException | SQLException e) {
	      e.printStackTrace();
	      return ;
	    } 
	    System.out.println("Log records " + record + " successfully processed.");
	}
	
	//Process event/log record
	public void processEvent(Map<String, Event> events, Event event, Database db) throws SQLException {
    	Long startTimeStamp;
    	Long endTimeStamp;
    	
    	/*
    	 * I am using HashMap instead of writing record in the file/database to prevent I/O operation
    	 * to pull record from file/database.
    	 * 
    	 * I do remove key from HashMap once event is START/FINISHED records match so it will 
    	 * not go out of memory.
    	 */
    	
        //Check whether event already exists in the MAP
        Event existEvent = events.get(event.getId());
        
        //If event exists then it will complete event start/finished log
        if (existEvent != null) {
        	//get start and end time 
    		startTimeStamp = existEvent.getState().equals("STARTED") ? existEvent.getTimestamp() : event.getTimestamp();
    		endTimeStamp = existEvent.getState().equals("STARTED") ? event.getTimestamp() : existEvent.getTimestamp();
    		
    		//Generate event log
    		this.generateEventLog(db, event.getId(), endTimeStamp - startTimeStamp);
    		
    		events.remove(event.getId());	//Free up memory
        } else {
        	//Put new event to MAP
        	events.put(event.getId(), event);
        }		
	}
	
	//Generate event log/record in the HSQLDB database
	public void generateEventLog(Database db, String id, Long duration) throws SQLException {
		db.insertEventLog(id, duration, duration > 3 ? "true" : "false");
		System.out.println("Event log record successfully added for " + id);
	}
}
