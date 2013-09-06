package chord;

import java.io.ByteArrayInputStream;
import chord.Commons;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.TreeSet;

import javax.swing.event.DocumentEvent.ElementChange;

import chord.ClientGetMessage.ClientGetMsg.Builder;
import com.rabbitmq.client.AMQP.BasicProperties;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class Sender3 implements Runnable{

	ConnectionFactory factory;
	Connection connection ;
	Channel channel = null;
	String corrId = null;
	int myNo ;
	String qName = null;
	HashMap<Integer, String> queueMappings = null;
	int total;
	int[][] chordLog ;
	int chordSize ;
	BufferedReader br = null;
	
	public Sender3(int myNo, String qName, int total) throws IOException
	{
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setPort(5673);
		connection = factory.newConnection();
		channel = connection.createChannel();
		br = new BufferedReader(new InputStreamReader(System.in));
		this.myNo = myNo;
		this.qName = qName;
		channel.queueDeclare(qName, false, false, false, null);
		channel.exchangeDeclare(Commons.EXCHANGE_NAME, "fanout");
		channel.queueBind(qName, Commons.EXCHANGE_NAME, "");
		System.out.println(" Binding "+qName +" to "+Commons.EXCHANGE_NAME);
		queueMappings = new HashMap<Integer, String>();
		queueMappings.put(myNo, qName);
		this.total = total;
		chordSize= (int)Math.ceil(Math.log(total));
		chordLog = new int[chordSize][2];
		System.out.println(" max "+chordSize);
		for(int i = 0; i < chordSize; i++ )
		{
			//int tempid = i+myNo;
			int id2 = (int) (Math.pow(2,i)+myNo);
			chordLog[i][0] = id2%total;
			chordLog[i][1] = myNo;
		}
		System.out.println("Log ");
		for(int i = 0; i < chordSize; i++ )
		{
			System.out.println(" "+chordLog[i][0]+" "+chordLog[i][1]);
		}
		
		sendInitMsg();
		
	}
	
	
	public void sendInitMsg() throws IOException
	{
		System.out.print("Press Enter to send Init Msg ");
		String msg = br.readLine();
		channel.basicPublish( Commons.EXCHANGE_NAME, "", null, "test msg".getBytes());
		
		ChordMessage.ChordMsg getChordMsg = Commons.getInitMessage(myNo,qName);

		ByteArrayOutputStream oStream = new ByteArrayOutputStream();
		getChordMsg.writeTo(oStream);

		sendBcastMessage(oStream);
	}

	public void sendMsg() throws IOException
	{
		
		
	//	while(true){	
			System.out.print("Enter Message ");
			String msg = br.readLine();

			ChordMessage.ChordMsg getChordMsg = Commons.getInitMessage(myNo,br.readLine());

			ByteArrayOutputStream oStream = new ByteArrayOutputStream();
			getChordMsg.writeTo(oStream);

			sendBcastMessage(oStream);
	//	}
	}


	public void run()
	{
		try {
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(qName, true, consumer);
			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				System.out.println("received  "+delivery);

				ByteArrayInputStream iStream = new ByteArrayInputStream(delivery.getBody());
				ChordMessage.ChordMsg.Builder builder = ChordMessage.ChordMsg.newBuilder();
				builder.mergeFrom(iStream);
				ChordMessage.ChordMsg data = builder.build();

				System.out.println(" [x] Received '" + data + "'");
				String msgType = data.getMsgType();
				if(msgType.equalsIgnoreCase("ClientGetMessage"))
				{
					ClientGetMessage.ClientGetMsg msg = data.getClientGetMsg();
					this.handleClientGetMessage(msg);

				}
				else if(msgType.equalsIgnoreCase("InitMessage"))
				{
					InitMessage.InitMsg msg = data.getInitMsg();
					this.handleInitMessage(msg);

				}
				else if(msgType.equalsIgnoreCase("InitAckMessage"))
				{
					InitAckMessage.InitAckMsg msg = data.getInitActMsg();
					this.handleInitAckMessage(msg);

				}
				if(msgType.equalsIgnoreCase("ClientSetMessage"))
				{
					ClientSetMessage.ClientSetMsg msg = data.getClientSetMsg();
					this.handleClientSetMessage(msg);

				}
				if(msgType.equalsIgnoreCase("ClientResponseMessage"))
				{
					ClientResponseMessage.ClientResponseMsg msg = data.getClientResponseMsg();
					this.handleClientResponseMessage(msg);

				}
				else{
					System.out.println(" Unknown type");
					break;
				}
			}
			channel.close();
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ShutdownSignalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void handleInitMessage(InitMessage.InitMsg msg)
	{
		int nodeNo = msg.getMyno();
		String nodeQName = msg.getQname();

		queueMappings.put(nodeNo, nodeQName);
		
		
		for(int i = 0; i < chordSize; i++)
		{
			chordLog[i][1] = findSucc(chordLog[i][0], myNo, chordLog[i][1], nodeNo);
		}
	}
	
	private int findSucc(int curNo,int succ, int myNo, int newNo)
	{
		for(int i = myNo+1; i<= curNo; i = (i+1)%total)
		{
			if( i == newNo) return newNo;
			if( i == succ) return succ;
		}
		return succ;
	}
	public void handleInitAckMessage(InitAckMessage.InitAckMsg msg)
	{
		
	}
	
	public void handleClientGetMessage(ClientGetMessage.ClientGetMsg msg)
	{
		
	}
	
	
	public void handleClientSetMessage(ClientSetMessage.ClientSetMsg msg)
	{
		
	}
	
	public void handleClientResponseMessage(ClientResponseMessage.ClientResponseMsg msg)
	{
		
	}
	
	
	public void sendBcastMessage(ByteArrayOutputStream message) throws IOException
	{
		channel.basicPublish( Commons.EXCHANGE_NAME, "", null, message.toByteArray());
	}

	public static void main(String[] argv) throws Exception {

		//System.out.println(" "+argv[0]);
		int myNo = Integer.parseInt(argv[0]);
		int totalNodes = Integer.parseInt(argv[2]);
		Sender3 obj = new Sender3(myNo, argv[1], totalNodes);
		new Thread(obj).start();
		obj.sendMsg();

	}


}