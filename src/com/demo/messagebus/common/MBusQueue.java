package com.demo.messagebus.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

public class MBusQueue {
	private Queue<Message> queue;
	private String name = null;
	
	public MBusQueue(Queue<Message> queue){
		this.queue = queue;
	}
	
	public void add(Message msg){
		this.queue.offer(msg);
	}
	
	public Message poll(){
		return this.queue.poll();
	}
	
	public void list(){
		Iterator<Message> itr = this.queue.iterator();
		Message m;
		while(itr.hasNext()){
			m = itr.next();
			System.out.println(m.toString());
			System.out.println("---------------------------------------------------------");
		}
	}
	
	public List<Message> fetch(int n){
		List<Message> msgs = new ArrayList<Message>();
		while(n > 0 && this.queue.size() > 0){
			msgs.add(this.queue.poll());
			n--;
		}
		System.out.println("Returning "+msgs.size()+", messages");
		for(int i=0;i<msgs.size();i++){
			System.out.println(msgs.get(i).toString()+"\n\n");
		}
		return msgs;
	}
	
	public int size(){
		return this.queue.size();
	}
	
	public String name(){
		if(this.name != null)
			return this.name;
		return "Unknown";
	}
}
