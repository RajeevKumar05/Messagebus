package com.demo.messagebus.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


public class MBusQueueFactory {
	
	public static Map<String,MBusQueue> queues = new HashMap<String,MBusQueue>();
	
	public static void createTopicQueue(String topic){
		if(queues.get(topic) == null)
			queues.put(topic, new MBusQueue(new LinkedList<Message>()));
	}
	
	public static void createQueues(){
		//ArrayList<String> topics = getQueueConfiguration();
		String topic = "my.test.topic";
		createTopicQueue(topic);
	}
	
	public static MBusQueue getQueue(String topic) throws QueueNotFoundException{
		MBusQueue queue = queues.get(topic);
		if(queue == null)
			throw new QueueNotFoundException("Queue for topic - "+topic+" is not configured. Contact administrator.");
		return queue;
	}
	
}
