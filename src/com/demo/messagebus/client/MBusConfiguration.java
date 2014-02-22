package com.demo.messagebus.client;

import com.demo.messagebus.common.Constants;
import com.demo.messagebus.common.MBusQueueFactory;
import com.demo.messagebus.common.MBusWorkerFactory;

public class MBusConfiguration {
	
	public static void initializeClientWorker() throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		MBusWorkerFactory.addWorker(System.getProperty(Constants.MESSAGEBUS_TOPIC), System.getProperty(Constants.MESSAGEBUS_CLIENT_WORKER));
	}
	
	public static void initializeClientQueue(){
		MBusQueueFactory.createTopicQueue(System.getProperty(Constants.MESSAGEBUS_TOPIC));
	}
	
	public static void initConfig(){
		System.setProperty(Constants.MESSAGEBUS_TOPIC, "my.test.topic");
		System.setProperty(Constants.MESSAGEBUS_CLIENT_WORKER, "com.demo.messagebus.client.MBusClient");
		System.setProperty(Constants.ONLINE_CLIENT,"TRUE");
	}

}
