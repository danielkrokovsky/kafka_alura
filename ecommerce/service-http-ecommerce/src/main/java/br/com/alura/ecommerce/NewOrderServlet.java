package br.com.alura.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class NewOrderServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();
	private final KafkaDispatcher<String>  emailDispatcher = new KafkaDispatcher<String>();
	
	
	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		super.destroy();
		orderDispatcher.close();
		emailDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

				try {
					
					//http://localhost:8080/new?email=danielkrokovsky@hotmail.com&amount=153
					var email = req.getParameter("email");
					var amount = new BigDecimal(req.getParameter("amount"));
					var orderId = UUID.randomUUID().toString();
					var order = new Order(orderId, amount, email);

					orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

					var emailCode = "Thank you for your order! We are processing your order!";
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
					
					System.out.println("Processo da compra finalizado");
					
					resp.getWriter().print("Processo da compra finalizado");
					resp.setStatus(HttpServletResponse.SC_OK);

				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					throw new ServletException(e);
				}
	}
}
