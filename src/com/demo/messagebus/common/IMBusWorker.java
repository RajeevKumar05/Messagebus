package com.demo.messagebus.common;

public interface IMBusWorker {
	public void process(Message m) throws Exception;
	public String test();
}
