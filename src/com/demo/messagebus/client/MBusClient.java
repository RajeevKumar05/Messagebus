package com.demo.messagebus.client;

import com.demo.messagebus.common.IMBusWorker;
import com.demo.messagebus.common.Message;

public class MBusClient implements IMBusWorker{
	
	@Override
	public void process(Message m) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Processing Message : "+m.toString());	
	}

	@Override
	public String test() {
		// TODO Auto-generated method stub
		return "Test Succeeded";
	}
	

}
