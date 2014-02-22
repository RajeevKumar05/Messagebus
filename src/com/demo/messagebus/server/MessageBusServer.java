package com.demo.messagebus.server;

import java.net.*;
import java.util.concurrent.SynchronousQueue;
import java.io.*;

import java.util.LinkedList;
import com.demo.messagebus.common.Message;

public class MessageBusServer {
	public static LinkedList<Message> queue = new LinkedList<Message>();
    public static void main(String[] args) throws IOException {
    	int port = 4444;
        ServerSocket serverSocket = null;
        try {
        	String _port = System.getProperty("port");
        	if(_port != null)
        		port = Integer.parseInt(_port);
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            System.err.println("Could not listen on port: "+port+".");
            System.exit(1);
        }
        System.out.println("Server is running at port : "+port+".");
        
        Socket clientSocket = null;
       while(true){
    	   try {
    		   System.out.println("Waiting for client.....");
               clientSocket = serverSocket.accept();
           } catch (IOException e) {
               System.err.println("Accept failed.");
               e.printStackTrace();
           }        
           System.out.println(clientSocket.getInetAddress().toString()+" Connected");           
           Runnable mt = new MessageBusWorker(clientSocket);
           mt.run();
       }
    }
}
