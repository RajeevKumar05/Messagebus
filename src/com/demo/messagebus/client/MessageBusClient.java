package com.demo.messagebus.client;

import com.demo.messagebus.common.Constants;
import com.demo.messagebus.common.IMBusWorker;
import com.demo.messagebus.common.MBusQueue;
import com.demo.messagebus.common.MBusQueueFactory;
import com.demo.messagebus.common.MBusWorkerFactory;

public class MessageBusClient extends Thread{
	@Override
	public void run() {
		/*
		try {
			createConnectionToMBusClientAndListen();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		*/
	}

	public static void printProperties(){
		System.out.println(Constants.MESSAGEBUS_TOPIC+" = "+System.getProperty(Constants.MESSAGEBUS_TOPIC));
	}

	public static void main(String[] s) throws InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException{
		MBusConfiguration.initConfig();
		MBusConfiguration.initializeClientWorker();

		Thread clientThread = new Thread(new MessageBusClient());

		MBusQueue queue = MBusQueueFactory.getQueue(System.getProperty(Constants.MESSAGEBUS_TOPIC));

		IMBusWorker worker = MBusWorkerFactory.getWorker(System.getProperty(Constants.MESSAGEBUS_TOPIC));

		Thread workerThread = new Thread(new MBusClientWorker(queue,worker));
		clientThread.start();
		workerThread.start();
		clientThread.join();
		workerThread.join();
	}

}
