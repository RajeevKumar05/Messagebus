package com.demo.messagebus.server;

import java.io.IOException;
import java.net.*;

import com.demo.messagebus.common.Client;
import com.demo.messagebus.common.Message;
import com.demo.messagebus.common.MessageBusProducer;

public class NotifierClient implements Client {
	
	String host = null;
	int port = 0;
	
	public NotifierClient(String host,int port){
		this.host = host;
		this.port = port;
		System.out.println("New client created at Host: "+host+", Port = "+port);
	}
	
	@Override
	public boolean push(Message m) {
		System.out.println("Pushing message to client !!");
		try {
			MessageBusProducer.writeToSocket(this.connect(), m);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return this.toString();
	}
	
	public Socket connect() throws UnknownHostException, IOException{
		return new Socket(this.host,this.port);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
