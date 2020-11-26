package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudeDetectorService {
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();

	public static void main(String[] args) {

		var fraudService = new FraudeDetectorService();
		try (var service = new KafkaService<Order>(FraudeDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				fraudService::parse, Order.class, new HashMap<String, String>())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Order> record) throws InterruptedException, ExecutionException {
		System.out.println("---------------------------------------------------------");
		System.out.println("Processando new Order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println("partition:" + record.partition());
		System.out.println(record.offset());
		System.out.println("Order is: "+record.value().getAmount());
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		var order = record.value();

		if (isFraud(order)) {
			System.out.println("Order is a fraud");
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
		} else {

			System.out.println("Approved: "+record.value());

			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
		}

	}
	
	 

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}
}
