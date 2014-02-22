package com.demo.messagebus.client;

import com.demo.messagebus.common.IMBusWorker;
import com.demo.messagebus.common.MBusQueue;

public class MBusClientWorker extends Thread {
	
	MBusQueue queue;
	IMBusWorker worker;
	
	public MBusClientWorker(MBusQueue queue, IMBusWorker worker){
		this.queue = queue;
		this.worker = worker;
	}

	@Override
	public void run() {
		System.out.println("Client Worker: "+this.worker.getClass().getName()+" started.");
		while(true){
			while(this.queue.size()>0){
				try {
					this.worker.process(this.queue.poll());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
