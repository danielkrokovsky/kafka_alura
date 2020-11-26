package br.com.alura.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CreateUserService {

	private Connection connection;

	CreateUserService() throws SQLException{
		
		String url = "jdbc:sqlite:target/users_database.db";
		connection = DriverManager.getConnection(url);
		
		try {
		connection.createStatement().execute("create table users ("+
			    "uuid varchar(200) primary key,"+
			    "email varchar(200))");
		
		}catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	public static void main(String[] args) throws SQLException {

		var fraudService = new CreateUserService();
		try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				fraudService::parse, Order.class, new HashMap<String, String>())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Order> record) throws SQLException {
		System.out.println("---------------------------------------------------------");
		System.out.println("Processando new Order, checking for new user");

		System.out.println(record.value());

		var order = record.value();

		if (isNewUser(order.getEmail())) {
			insertNewUser(order.getUserId(), order.getEmail());

		}
	}

	private void insertNewUser(String uuid, String email) throws SQLException {
		PreparedStatement insert = this.connection.prepareStatement("insert into Users (uuid, email) values (?,?)");
		insert.setString(1, uuid);
		insert.setString(2, email);
		insert.execute();
		System.out.println("Usuário uuid e "+email+" adicionado");
		
	}

	private boolean isNewUser(String email) throws SQLException {
		var exists = connection.prepareStatement("select uuid from Users where email = ? limit 1");
		exists.setString(1, email);
		ResultSet results = exists.executeQuery();
		
		return !results.next();
	}

}