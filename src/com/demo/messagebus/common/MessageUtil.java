package com.demo.messagebus.common;

import java.util.Iterator;
import java.util.List;

public class MessageUtil {
	public static String stringify(List<Message> messages){
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		if(messages != null){
			Iterator<Message> itr = messages.iterator();
			while(itr.hasNext()){
				sb.append(itr.next().toString());
				if(itr.hasNext()){
					sb.append(",");
				}
			}
		}
		sb.append("]");
		return sb.toString();
	}
}
