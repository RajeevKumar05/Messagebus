package com.demo.messagebus.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

import org.json.JSONException;

import com.demo.messagebus.common.Client;
import com.demo.messagebus.common.Constants;
import com.demo.messagebus.common.MBusQueueFactory;
import com.demo.messagebus.common.Message;

public class MessageBus {
	
	public static HashMap<String,List<Client>> clientStore = new HashMap<String,List<Client>>();
	
	
	public static boolean addClient(Message m) throws NumberFormatException, JSONException{
		String topic = m.topic();
		System.out.println("Adding client for topic = "+m.topic());
		if(clientStore.get(topic) == null){
			List<Client> clnts = new ArrayList<Client>();
			clnts.add(new NotifierClient(m.get(Constants.MESSAGEBUS_CLIENT_HOST),Integer.parseInt(m.get(Constants.MESSAGEBUS_CLIENT_PORT))));
			clientStore.put(topic, clnts);
		}else{
			clientStore.get(topic).add(new NotifierClient(m.get(Constants.MESSAGEBUS_CLIENT_HOST),Integer.parseInt(m.get(Constants.MESSAGEBUS_CLIENT_PORT))));
		}
		return true;
	}
	
	public static List<Message> process(Message m) throws JSONException{
		if(m.containsKey("isRegistration") && m.get("isRegistration") != null && m.get("isRegistration").equalsIgnoreCase("YES")){
			addClient(m);
			return null;
		}else if(m.containsKey(Constants.MESSAGEBUS_COMMAND) && m.get(Constants.MESSAGEBUS_COMMAND).equalsIgnoreCase(Constants.FETCH_MESSAGE)){
			return MBusQueueFactory.getQueue(m.topic()).fetch(2);
		}else{
			MBusQueueFactory.createTopicQueue(m.topic()).add(m);
			MessageHandler mh = new MessageHandler(m,clientStore.get(m.topic()));
			mh.sendMessage();
			return null;
		}
	}
}
