package chord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import chord.ClientGetMessage.ClientGetMsg.Builder;
import com.rabbitmq.client.AMQP.BasicProperties;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class Sender2{

  private final static String QUEUE_NAME = "hello";

  public static void main(String[] argv) throws Exception {
      	      
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setPort(5673);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    ClientGetMessage.ClientGetMsg.Builder getMsgBuilder = ClientGetMessage.ClientGetMsg.newBuilder();
    getMsgBuilder.setKey("hello");
    ClientGetMessage.ClientGetMsg  getMsg = getMsgBuilder.build();
    ChordMessage.ChordMsg.Builder getChordMsgBuilder = ChordMessage.ChordMsg.newBuilder();
    getChordMsgBuilder.setClientGetMsg(getMsg);
    getChordMsgBuilder.setMsgType("ClientGetMessage");
    ChordMessage.ChordMsg getChordMsg = getChordMsgBuilder.build();
    
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    getChordMsg.writeTo(oStream);
    
   // String corrId = java.util.UUID.randomUUID().toString();
    BasicProperties props = new BasicProperties
            .Builder()
            .replyTo("ack_hello")
            .correlationId("123456789")
            .build();
    
   
    channel.queueDeclare("ack_hello", false, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    
    QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume("ack_hello", true, consumer);
    
    
   // String message = "Hello World!";
    channel.basicPublish("", QUEUE_NAME, props, oStream.toByteArray());
    System.out.println(" [x] Sent '" + oStream + "'");
    
    
    while (true) {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        System.out.println("received  "+delivery);
        if (delivery.getProperties().getCorrelationId().equals("123456789")) {
           String  response = new String(delivery.getBody());
           System.out.println(" Reposnse :: "+response);
            break;
        }
    }

    channel.close();
    connection.close();
  }
  
  
  
}