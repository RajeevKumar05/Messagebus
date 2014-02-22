package com.demo.messagebus.common;

import java.util.HashMap;
import java.util.Map;

import com.demo.messagebus.common.IMBusWorker;

public class MBusWorkerFactory {
	
	private static Map<String,IMBusWorker> clients = new HashMap<String,IMBusWorker>();
	
	public static void addWorker(String topic,String className) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		System.out.println("Instantiating Worker "+className+", for topic "+topic);
		IMBusWorker worker = (IMBusWorker)Class.forName(className).newInstance();
		System.out.println(worker.test());
		clients.put(topic, worker);
	}
	
	public static IMBusWorker getWorker(String topic){
		System.out.println("Returning worker for topic : "+topic);
		return clients.get(topic);
	}
}
