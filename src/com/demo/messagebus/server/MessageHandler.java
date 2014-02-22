package com.demo.messagebus.server;

import java.util.List;

import com.demo.messagebus.common.Client;
import com.demo.messagebus.common.Message;

public class MessageHandler {
	
	Message msg;
	List<Client> clients;
	
	public MessageHandler(Message m, List<Client> clients){this.msg = m; this.clients = clients;}
	
	public void sendMessage() {
		/*for(Client client : clients){
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				System.out.println("Pushing to Queue : "+client.name()+"\nPayload = "+this.msg.toString()+"\nTopic = "+this.msg.topic());
			} catch (JSONException e) {
				e.printStackTrace();
			}
			System.out.println("-------------------------------------------");
		}*/
		System.out.println("********************************************");
		System.out.println("Received Message : "+msg.toString());
		for(Client client: clients){
			client.push(msg);
		}
		System.out.println("Queue size = "+MessageBusServer.queue.size());
		System.out.println("********************************************");
	}

}
