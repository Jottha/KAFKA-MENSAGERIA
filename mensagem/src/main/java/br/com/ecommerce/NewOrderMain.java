/**
 * 
 */
package br.com.ecommerce;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author jotth
 *
 */
public class NewOrderMain {

	public static void main(String[] args) {

		var producer = new KafkaProducer<String, String>(properties());
		
		for (int i = 0; i < 100; i++) {
			
			var key = UUID.randomUUID().toString();
			var value = key.concat(",67523,7894589745");
			var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
			var email = "Thenk you for your order! We are processing your order!";
			var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
			try {
				Callback callback = (data, ex) -> {

					if (ex != null) {
						ex.printStackTrace();
					}
					System.out.println("sucesso enviando" + data.topic() + ":::partition " + data.partition()
							+ "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
				};
				producer.send(record, callback).get();
				producer.send(emailRecord, callback).get();
			} catch (InterruptedException e) {

				e.printStackTrace();
				System.out.println("Interrupção do tópico!");
			} catch (ExecutionException e) {

				e.printStackTrace();
				System.out.println("Execução do tópico parada!");
			} 
		}
	}

	private static Properties properties() {

		var properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return properties;
	}

}
