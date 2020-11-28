package br.com.alura.ecommerce;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonDeserializer<T> implements Deserializer<Message>{
	
	//public static final String TYPE_CONFIG = "br.com.alura.ecommerce.type_config";
	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();
	private Class<T> type;
	
	/*
	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
		String typeName = String.valueOf(configs.get(TYPE_CONFIG));
		try {
			this.type = (Class<T>) Class.forName(typeName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("error type Deserializer");
		}
		
	}
	*/

	@Override
	public Message deserialize(String topic, byte[] bytes) {
		
		return gson.fromJson(new String(bytes), Message.class);
	}

}
