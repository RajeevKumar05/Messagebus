package com.demo.messagebus.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;

import com.demo.messagebus.common.Constants;
import com.demo.messagebus.common.Message;
import com.demo.messagebus.common.MessageBusProducer;
import com.demo.messagebus.common.MessageUtil;

public class MessageBusWorker extends Thread {
	Socket clientSocket;
	BufferedReader in;
	MessageBusProducer producer;
	public MessageBusWorker(Socket socket) throws IOException{
		this.clientSocket = socket;
		this.producer = new MessageBusProducer(socket);
	}
	public void run(){
		try{
			in = new BufferedReader(
					new InputStreamReader(
							clientSocket.getInputStream()));
			String inputLine; 
			StringBuilder input = new StringBuilder();
			Message msg = null;
			while ((inputLine = in.readLine()) != null) {
				if (inputLine.equalsIgnoreCase("EOM")){
					msg = MessageBus.process(new Message(input.toString()));
					this.producer.produce(msg, true);
					input = new StringBuilder();
				}else if(inputLine.equalsIgnoreCase("BYE")){
					msg = MessageBus.process(new Message(input.toString()));
					this.producer.produce(msg, false);
					break;
				}else{
					input.append(inputLine);
				}   
			}
		}catch(IOException ioe){
			ioe.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}finally{
			try{
				in.close();
				clientSocket.close();
				this.producer.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
