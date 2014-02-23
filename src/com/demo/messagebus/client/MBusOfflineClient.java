package com.demo.messagebus.client;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;

import com.demo.messagebus.common.Constants;
import com.demo.messagebus.common.MBusQueue;
import com.demo.messagebus.common.MBusQueueFactory;
import com.demo.messagebus.common.Message;
import com.demo.messagebus.common.MessageBusProducer;
import com.demo.messagebus.common.QueueNotFoundException;

public class MBusOfflineClient {
	private String host = "localhost";
	private int port = 4444;
	
	public MBusOfflineClient() throws IOException{
		this.createConnectionAndListen();
	}
	
	public MBusOfflineClient(String host,int port) throws IOException{
		this.host = host;
		this.port = port;
		this.createConnectionAndListen();
	}
	
	private void createConnectionAndListen() throws IOException{
		Socket pipe = null;
		BufferedReader in = null;
		MBusQueueFactory.createTopicQueue(System.getProperty(Constants.MESSAGEBUS_TOPIC));
		try {
			pipe = new Socket(this.host, this.port);
			in = new BufferedReader(new InputStreamReader(pipe.getInputStream()));
		} catch (UnknownHostException e) {
			System.err.println("Don't know about host: "+this.host);
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: "+this.host);
			System.exit(1);
		}
		Map<String,Object> m = new HashMap<String,Object>();
		m.put(Constants.MESSAGEBUS_TOPIC, System.getProperty(Constants.MESSAGEBUS_TOPIC));
		m.put(Constants.MESSAGEBUS_COMMAND, Constants.FETCH_MESSAGE);
		Message command = new Message(m);
		MessageBusProducer producer = new MessageBusProducer(pipe);
		StringBuilder fromServer = new StringBuilder();
		String line;
		try{
			fromServer = new StringBuilder();
			producer.produce(command, true);
			System.out.println("************************************");
			System.out.println("Fetching messages.....");
			Thread.sleep(2000);
			while ((line = in.readLine()) != null) {
				if (line.equalsIgnoreCase("EOM")){
					System.out.println("Client received : "+fromServer.toString());
					System.out.println("************************************");
					Message msg = new Message(fromServer.toString());
					MBusQueue queue = MBusQueueFactory.getQueue(System.getProperty(Constants.MESSAGEBUS_TOPIC));
					Message mm;
					if(queue != null){
						JSONArray messages = msg.getList(Constants.MESSAGE_LIST);
						for(int i=0;i<messages.length();i++){
							mm = new Message(messages.getJSONObject(i));
							System.out.println("Queuing in Message : "+mm.toString());
							queue.add(mm);
						}
					}
					fromServer = new StringBuilder();
					producer.produce(command, true);
					System.out.println("************************************");
					System.out.println("Fetching messages.....");
					Thread.sleep(2000);
				}else{
					fromServer.append(line);
				}
			}
		}catch (JSONException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (QueueNotFoundException e) {
			e.printStackTrace();
		}finally{
			try {
				producer.close();
				in.close();
				pipe.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] s) throws IOException{
		System.setProperty(Constants.MESSAGEBUS_TOPIC, "my.test.topic");
		new MBusOfflineClient();
	}
}
