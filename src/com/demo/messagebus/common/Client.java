package com.demo.messagebus.common;


public interface Client {
	public boolean push(Message m);
	public String name();
}
