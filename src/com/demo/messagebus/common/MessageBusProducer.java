package com.demo.messagebus.common;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

import com.demo.messagebus.client.MBusConfiguration;


public class MessageBusProducer {
	private Socket pipe = null;
	private PrintWriter out = null;
	
	public MessageBusProducer(Socket pipe) throws IOException{
		this.pipe = pipe;
		out = new PrintWriter(pipe.getOutputStream(), true);
	}
	
	public MessageBusProducer(String host,int port) throws IOException{
		this.pipe = new Socket(host, port);
		out = new PrintWriter(pipe.getOutputStream(), true);
	}
	
	public void produce(Message message, boolean isMore) throws IOException{
		InputStream is = null;
		BufferedReader br = null;
		try{
			is = new ByteArrayInputStream(message.toString().getBytes());
			br = new BufferedReader(new InputStreamReader(is));
			StringBuilder msg = new StringBuilder();

			String line = br.readLine();
			while(line != null){
				msg.append(line);
				line = br.readLine();
			}
			out.println(msg);
			if(isMore)
				out.println("EOM");
			else
				out.println("BYE");
		}catch(IOException ioe){
			throw ioe;
		}finally{
			if(br != null)
				br.close();
			if(is != null)
				is.close();
		}
	}
	
	public void close(){
		try {
			this.pipe.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.out.close();
	}
	public static void produce(String host,int port, Message message) throws IOException{
		Socket mbusSocket = null;
		PrintWriter out = null;
		try {
			mbusSocket = new Socket(host, port);
			out = new PrintWriter(mbusSocket.getOutputStream(), true);
		} catch (UnknownHostException e) {
			System.err.println("Don't know about host: "+host+", port: "+port);
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: "+host+", port: "+port);
			System.exit(1);
		}
		InputStream is = new ByteArrayInputStream(message.toString().getBytes());
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		StringBuilder msg = new StringBuilder();

		String line = br.readLine();
		while(line != null){
			System.out.println("Line = "+line);
			msg.append(line);
			line = br.readLine();
		}
		out.println(msg);
		out.println("BYE");
		out.close();
		br.close();
		mbusSocket.close();
	}
	
	public static void writeToSocket(Socket pipe,Message message) throws IOException{
		System.out.println("Writing data to client socket");
		PrintWriter out = new PrintWriter(pipe.getOutputStream(), true);
		InputStream is = new ByteArrayInputStream(message.toString().getBytes());
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		StringBuilder msg = new StringBuilder();

		String line = br.readLine();
		while(line != null){
			//System.out.println("Line = "+line);
			msg.append(line);
			line = br.readLine();
		}
		out.println(msg);
		out.println("BYE");
		
		//Closing streams
		out.close();
		br.close();
		pipe.close();
	}
	
	public static void sendToSocket(Socket pipe,Message message) throws IOException{
		System.out.println("Writing data to client socket");
		PrintWriter out = new PrintWriter(pipe.getOutputStream(), true);
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
	}

	public static void main_test(String[] args) throws IOException {

		Socket kkSocket = null;
		PrintWriter out = null;
		BufferedReader in = null;

		try {
			kkSocket = new Socket("localhost", 4444);
			out = new PrintWriter(kkSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));
		} catch (UnknownHostException e) {
			System.err.println("Don't know about host: taranis.");
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: taranis.");
			System.exit(1);
		}

		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String fromServer;
		String fromUser;

		while ((fromServer = in.readLine()) != null) {
			System.out.println("Server: " + fromServer);
			if (fromServer.equals("BYE."))
				break;

			fromUser = stdIn.readLine();
			if (fromUser != null) {
				System.out.println("Client: " + fromUser);
				out.println(fromUser);
			}
		}

		out.close();
		in.close();
		stdIn.close();
		kkSocket.close();
	}
	
	public static void main(String[] s) throws IOException{
		MBusConfiguration.initConfig();
		//System.setProperty(Constants.MESSAGEBUS_TOPIC,"my.another.test.topic");
		Map<String,Object> m = new HashMap<String,Object>();
		Map<String,Object> message = new HashMap<String,Object>();
		message.put("firstName", "Rajeev");
		message.put("lastName", "Kumar");
		m.put(Constants.MESSAGE, message.toString());
		m.put(Constants.MESSAGEBUS_TOPIC, System.getProperty(Constants.MESSAGEBUS_TOPIC));
		Message msg = new Message(m);
		System.out.println("Producing : "+m.toString());
		MessageBusProducer.produce("localhost", 4444, msg);
	}
}