package com.demo.messagebus.client;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONException;

import com.demo.messagebus.common.Constants;
import com.demo.messagebus.common.MBusQueue;
import com.demo.messagebus.common.MBusQueueFactory;
import com.demo.messagebus.common.Message;
import com.demo.messagebus.common.MessageBusProducer;

public class MBusOnlineClient {
	
	private String serverHost = "localhost";
	private int serverPort = 4444;
	
	private String onlineClientHost = "localhost";
	private int onlineClientPort = 9673;
	
	private String topic = "my.test.topic";
	
	public MBusOnlineClient(){
		this.startAndRegisterClientToMBusServer();
	}
	
	public MBusOnlineClient(int onlineClientPort){
		this.onlineClientPort = onlineClientPort;
		
		String _host = System.getProperty(Constants.MESSAGEBUS_SERVER_HOST);
		this.serverHost = _host != null ? _host : this.serverHost ;
		String _port = System.getProperty(Constants.MESSAGEBUS_SERVER_PORT);
		this.serverPort = _port != null ? Integer.parseInt(_port) : this.serverPort ;
		
		this.startAndRegisterClientToMBusServer();
	}
	
	public void startAndRegisterClientToMBusServer(){
		MBusConfiguration.initConfig();
		MBusConfiguration.initializeClientQueue();
		ServerSocket clientSocket = null;
		try {
			clientSocket = new ServerSocket(this.onlineClientPort);
			this.registerOnlineClient();
		} catch (IOException e) {
			System.err.println("MBus client could not listen on port: "+this.onlineClientPort+".");
			System.exit(1);
		}
		System.out.println("MBus client is running at port : "+this.onlineClientPort+".");

		Socket serverSocket = null;
		while(true){
			try {
				System.out.println("Waiting for mbus server.....");
				serverSocket = clientSocket.accept();
			} catch (IOException e) {
				System.err.println("Accept failed.");
				e.printStackTrace();
			}        
			System.out.println(serverSocket.getInetAddress().toString()+" Connected");           
			try {
				List<Message> messages = this.readAndProcess(serverSocket);
				String serializedMessages = stringify(messages);
				Map<String,Object> hm = new HashMap<String,Object>();
				hm.put(Constants.MESSAGEBUS_TOPIC, this.topic);
				hm.put(Constants.MESSAGE_LIST, serializedMessages);
				Message m = new Message(hm);
				System.out.println("Sending : "+m.toString());
				if(messages != null){
					MessageBusProducer.writeToSocket(serverSocket, new Message(hm));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public String stringify(List<Message> messages){
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		if(messages != null){
			Iterator<Message> itr = messages.iterator();
			while(itr.hasNext()){
				sb.append(itr.next().toString());
				if(itr.hasNext()){
					sb.append(",");
				}
			}
		}
		sb.append("]");
		return sb.toString();
	}
	
	public void registerOnlineClient() throws IOException{
		
		Map<String,Object> mp = new HashMap<String,Object>();
		
		mp.put("isRegistration", "yes");
		mp.put(Constants.MESSAGEBUS_TOPIC, "my.test.topic");
		mp.put(Constants.MESSAGEBUS_CLIENT_HOST, this.onlineClientHost);
		mp.put(Constants.MESSAGEBUS_CLIENT_PORT, this.onlineClientPort);
		
		Message message = new Message(mp);
		
		Socket mbusSocket = null;
		PrintWriter out = null;
		try {
			mbusSocket = new Socket(this.serverHost, this.serverPort);
			out = new PrintWriter(mbusSocket.getOutputStream(), true);
		} catch (UnknownHostException e) {
			System.err.println("Don't know about mbus host: "+this.serverHost+", mbus port: "+this.serverPort);
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: "+this.serverHost+", port: "+this.serverPort);
			System.exit(1);
		}
		InputStream is = new ByteArrayInputStream(message.toString().getBytes());
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		StringBuilder msg = new StringBuilder();

		String line = br.readLine();
		while(line != null){
			msg.append(line);
			line = br.readLine();
		}
		out.println(msg);
		out.println("BYE");
		
		out.close();
		br.close();
		is.close();
		mbusSocket.close();
		
		System.out.println("Local client registered in online mode !!");
	}
	
	public List<Message> readAndProcess(Socket skt) throws IOException{
		BufferedReader in = new BufferedReader(
				new InputStreamReader(
						skt.getInputStream()));
		String inputLine; 
		StringBuilder input = new StringBuilder();

		while ((inputLine = in.readLine()) != null) {

			if (inputLine.equalsIgnoreCase("BYE")){
				return processMessage(input.toString());
			}
			input.append(inputLine);   
		}
		in.close();
		return null;
	}
	
	public static List<Message> processMessage(String input){
		System.out.println("************************************");
		System.out.println("Client received : "+input);
		Message msg = null;
		String topic = null;
		try {
			msg = new Message(input);
			topic = msg.topic();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		MBusQueue queue = MBusQueueFactory.getQueue(topic);
		if(queue != null){
			if(msg.containsKey(Constants.MESSAGEBUS_COMMAND)){
				try {
					if(msg.get(Constants.MESSAGEBUS_COMMAND) != null && msg.get(Constants.MESSAGEBUS_COMMAND).equalsIgnoreCase(Constants.FETCH_MESSAGE)){
						return queue.fetch(10);
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}else{
				queue.add(msg);
			}	
		}
		System.out.println("************************************");
		return null;
	}
	
	public static void main(String[] s){
		new MBusOnlineClient();
	}
}
