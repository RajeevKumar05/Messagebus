package com.demo.messagebus.server;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;

import com.demo.messagebus.common.Client;
import com.demo.messagebus.common.MBusQueueFactory;
import com.demo.messagebus.common.Message;
import com.demo.messagebus.common.QueueNotFoundException;

public class MessageHandler {
	
	Message msg;
	List<Client> clients;
	
	public MessageHandler(Message m, List<Client> clients){this.msg = m; this.clients = clients == null ? new ArrayList<Client>() : clients;}
	
	public void sendMessage() throws JSONException, QueueNotFoundException {
		System.out.println("********************************************");
		System.out.println("Received Message : "+msg.toString());
		for(Client client: clients){
			client.push(msg);
		}
		System.out.println("Queue size = "+MBusQueueFactory.getQueue(msg.topic()).size());
		System.out.println("********************************************");
	}

}
