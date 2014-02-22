package com.demo.messagebus.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.json.JSONException;

import com.demo.messagebus.common.Message;

public class MessageBusWorker implements Runnable {
	Socket clientSocket;
	BufferedReader in;
	public MessageBusWorker(Socket socket){
		this.clientSocket = socket;
	}
	public void run(){
		try{
			in = new BufferedReader(
					new InputStreamReader(
							clientSocket.getInputStream()));
			String inputLine; 
			StringBuilder input = new StringBuilder();

			while ((inputLine = in.readLine()) != null) {

				if (inputLine.equalsIgnoreCase("BYE")){
					break;
				}
				input.append(inputLine);   
			}
			MessageBus.process(new Message(input.toString()));
		}catch(IOException ioe){
			ioe.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}finally{
			try{
				in.close();
				clientSocket.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
