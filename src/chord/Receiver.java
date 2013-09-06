package chord;

import java.io.ByteArrayInputStream;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class Receiver {

    private final static String QUEUE_NAME = "hello4";

    public static void main(String[] argv) throws Exception {

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setPort(5673);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    
    QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume(QUEUE_NAME, true, consumer);
    
    while (true) {
      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
      System.out.println(" Received ----- "+delivery);
      ByteArrayInputStream iStream = new ByteArrayInputStream(delivery.getBody());
      ChordMessage.ChordMsg.Builder builder = ChordMessage.ChordMsg.newBuilder();
      builder.mergeFrom(iStream);
      ChordMessage.ChordMsg data = builder.build();
      BasicProperties props = delivery.getProperties();
      BasicProperties replyProps = new BasicProperties
                                       .Builder()
                                       .correlationId(props.getCorrelationId())
                                       .build();
      
      System.out.println(" [x] Received '" + data + "'");
     if(data.getMsgType().equalsIgnoreCase("ClientGetMessage"))
     {
    	 System.out.println("props "+props.getCorrelationId());
    	 ClientGetMessage.ClientGetMsg msg = data.getClientGetMsg();
    	 System.out.println("Key "+msg.getKey());
    	 String response = "acknowledgement_"+msg.getKey();
    	 System.out.println("Responding for ClientGetMessage ");
    	   channel.basicPublish( "", props.getReplyTo(), replyProps, response.getBytes());

    	   channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
     }
     else{
    	 System.out.println(" Unknown type");
     }
    }
  }
}