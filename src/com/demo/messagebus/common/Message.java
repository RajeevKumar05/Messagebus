package com.demo.messagebus.common;

import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Message {
	JSONObject msg = new JSONObject();
	
	public Message(String s) throws JSONException{
		msg = new JSONObject(s);
	}
	
	public Message(Map<String,Object> msg){
		this.msg = new JSONObject(msg);
	}
	
	public Message(JSONObject msg){
		this.msg = msg;
	}
	
	public String get(String key) throws JSONException{
		Object val = msg.get(key);
		if(val != null)
			return ""+val;
		return null;
				
	}
	
	public JSONArray getList(String key){
		try {
			return new JSONArray(this.msg.getString(key));
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return new JSONArray();
	}
	
	public boolean containsKey(String key){
		if(msg.has(key))
			return true;
		else
			return false;
	}
	
	public String toString(){
		return msg.toString();
	}
	
	public String topic(){
		try {
			return msg.getString(Constants.MESSAGEBUS_TOPIC);
		} catch (JSONException e) {
			return null;
		}
	}
	
	public JSONObject payload() throws JSONException{
		return msg.getJSONObject("payload");
	}
	
	public static void main(String[] s) throws JSONException{
		JSONObject m = new JSONObject();
		m.put("fName", "Rajeev");
		m.put("lName", "Kumar");
		m.put("isWorking", true);
		JSONObject address = new JSONObject();
		address.put("city", "Chennai");
		address.put("state", "TN");
		address.put("pin", "600125");
		m.put("address", address);
		String[] places = {"Rannu Bigha","Nalanda","Patna","Bihar","Hey: \"Hello \"!!"};
		m.put("places", places);
		System.out.printf( "JSON: %s", m.toString(2) );
		String js = "{\"Name\":\"Rajeev Kumar\"}";
		JSONObject o = new JSONObject(js);
		System.out.println("--------------------------------");
		Iterator it = o.keys();
		while(it.hasNext()){
			String key = (String)it.next();
			System.out.println("Key = "+key+", Value = "+o.getString(key));
		}
	}
}
