package br.com.alura.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GenerateAllReportsServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<String>();

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		super.destroy();
		batchDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		try {
			
			batchDispatcher.send("SEND_MESSAGE_TO_ALL_USERS", "USER_GENERATE_READING_REPORT", "USER_GENERATE_READING_REPORT");

			System.out.println("Send generate to all users");
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().print("report request generated");

		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			throw new ServletException(e);
		}
	}
}