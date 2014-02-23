package com.demo.messagebus.common;

public class QueueNotFoundException extends Exception{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public QueueNotFoundException(){
		super("Queue Not Found !");
	}
	
	public QueueNotFoundException(String msg){
		super(msg);
	}
}
