package br.com.alura.ecommerce;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudeDetectorService {

	public static void main(String[] args) {

		var fraudService = new FraudeDetectorService();
		try (var service = new KafkaService<Order>(FraudeDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				fraudService::parse, Order.class, new HashMap<String, String>())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("---------------------------------------------------------");
		System.out.println("Processando new Order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println("partition:" + record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Order processed");
	}
}
