package com.demo.messagebus.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;

import org.json.JSONException;

import com.demo.messagebus.common.Client;
import com.demo.messagebus.common.Constants;
import com.demo.messagebus.common.MBusQueueFactory;
import com.demo.messagebus.common.Message;
import com.demo.messagebus.common.MessageUtil;
import com.demo.messagebus.common.QueueNotFoundException;

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
	
	public static Message process(Message m) throws JSONException{
		Map<String,Object> msg = new HashMap<String,Object>();
		try{
			if(m.containsKey("isRegistration") && m.get("isRegistration") != null && m.get("isRegistration").equalsIgnoreCase("YES")){
				addClient(m);
				msg.put("STATUS",Constants.SUCCESS);
				return new Message(msg);
			}else if(m.containsKey(Constants.MESSAGEBUS_COMMAND) && m.get(Constants.MESSAGEBUS_COMMAND).equalsIgnoreCase(Constants.FETCH_MESSAGE)){
				msg.put("STATUS",Constants.SUCCESS);
				msg.put(Constants.MESSAGE_LIST,MessageUtil.stringify(MBusQueueFactory.getQueue(m.topic()).fetch(2)));
				return new Message(msg);
			}else{
				//Adding only message not other headers
				MBusQueueFactory.getQueue(m.topic()).add(new Message(m.get(Constants.MESSAGE)));
				MessageHandler mh = new MessageHandler(m,clientStore.get(m.topic()));
				mh.sendMessage();
				msg.put("STATUS",Constants.SUCCESS);
				return new Message(msg);
			}
		}catch(QueueNotFoundException ex){
			msg.put("STATUS",Constants.ERROR);
			msg.put(Constants.MESSAGE, ex.getMessage());
			return new Message(msg);
		}
	}
}
