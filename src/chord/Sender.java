package chord;

import java.io.ByteArrayInputStream;

import chord.ChordMessage.ChordMsg;
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

public class Sender implements Runnable{

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
	
	public Sender(int myNo, String qName, int total) throws IOException
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
		
		
		//sendMsg();
		
	}
	
	
	public void sendInitMsg() throws IOException
	{
		System.out.print("Press Enter to send Init Msg ");
		String msg = br.readLine();
	//	channel.basicPublish( Commons.EXCHANGE_NAME, "", null, "test msg".getBytes());
		
		ChordMessage.ChordMsg getChordMsg = Commons.getInitMessage(myNo,qName);

		
		/*ByteArrayOutputStream oStream = new ByteArrayOutputStream();
		getChordMsg.writeTo(oStream);
		oStream.flush();*/
		
		sendBcastMessage(getChordMsg);
		//oStream.close();
	}

	public void sendMsg() throws IOException
	{
		
		
	//	while(true){	
			System.out.print("Enter Message ");
			String msg = br.readLine();

			ChordMessage.ChordMsg getChordMsg = Commons.getInitMessage(myNo,qName);

			ByteArrayOutputStream oStream = new ByteArrayOutputStream();
			getChordMsg.writeTo(oStream);
			oStream.flush();
			//oStream.close();
			//sendBcastMessage(oStream);
			oStream.close();
	//	}
	}


	public void run()
	{
		System.out.println(" Inside run method ");
		try {
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(qName, true, consumer);
			System.out.println("Consuming from "+qName);
			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				System.out.println("received  "+delivery);
				String message = new String(delivery.getBody());
				/*ByteArrayInputStream iStream = new ByteArrayInputStream(delivery.getBody());
				ChordMessage.ChordMsg.Builder builder = ChordMessage.ChordMsg.newBuilder();
				builder.mergeFrom(iStream);
				ChordMessage.ChordMsg data = builder.build();*/
				ChordMessage.ChordMsg data = Commons.Deserialize(message, ChordMessage.ChordMsg.class);

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
				else if(msgType.equalsIgnoreCase("ClientSetMessage"))
				{
					ClientSetMessage.ClientSetMsg msg = data.getClientSetMsg();
					this.handleClientSetMessage(msg);

				}
				else if(msgType.equalsIgnoreCase("ClientResponseMessage"))
				{
					ClientResponseMessage.ClientResponseMsg msg = data.getClientResponseMsg();
					this.handleClientResponseMessage(msg);

				}
				else{
					System.out.println("Last else "+data);
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

	public void handleInitMessage(InitMessage.InitMsg msg) throws IOException
	{
		System.out.println(" Handling Init Message ");
		int nodeNo = msg.getMyno();
		String nodeQName = msg.getQname();

		System.out.println("Received msg from "+nodeNo+" and "+nodeQName);
		if(!nodeQName.equalsIgnoreCase(qName)){
		queueMappings.put(nodeNo, nodeQName);
		
		
		for(int i = 0; i < chordSize; i++)
		{
			chordLog[i][1] = findSucc(chordLog[i][0], myNo, chordLog[i][1], nodeNo);
		}
		
		System.out.println("Log :: ");
		for(int i = 0; i < chordSize; i++ )
		{
			System.out.println(" "+chordLog[i][0]+" "+chordLog[i][1]);
		}
		System.out.println("Sending Init ack ");
		sendInitAckMessage();
		
		}
	}
	
	public void sendInitAckMessage() throws IOException
	{
		ChordMessage.ChordMsg getChordMsg = Commons.getInitAckMessage(myNo,qName);
		
		sendBcastMessage(getChordMsg);
	}
	
	private int findSucc(int curNo,int succ, int myNo, int newNo)
	{
		System.out.println(" curNo "+curNo+", succ "+succ+", myNo "+myNo+", newNo "+newNo);
		for(int i = myNo+1; i!= curNo; i = (i+1)%total)
		{
			if( i == newNo) return newNo;
			if( i == succ) return succ;
		}
		return succ;
	}
	
	public void handleInitAckMessage(InitAckMessage.InitAckMsg msg)
	{
		System.out.println(" Handling InitAck Message ");
		int nodeNo = msg.getMyno();
		String nodeQName = msg.getQname();
		System.out.println("Received msg from "+nodeNo+" and "+nodeQName);

		if(nodeQName.equalsIgnoreCase(qName)){
		queueMappings.put(nodeNo, nodeQName);
		
		
		for(int i = 0; i < chordSize; i++)
		{
			chordLog[i][1] = findSucc(chordLog[i][0], myNo, chordLog[i][1], nodeNo);
		}
		
		System.out.println("Log :: ");
		for(int i = 0; i < chordSize; i++ )
		{
			System.out.println(" "+chordLog[i][0]+" "+chordLog[i][1]);
		}
		}
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
	
	
	/*public void sendBcastMessage(ByteArrayOutputStream message) throws IOException
	{
		System.out.println("Sending bcast "+message );
		channel.basicPublish( Commons.EXCHANGE_NAME, "", null, message.toByteArray());
	}*/
	
	public void sendBcastMessage(ChordMsg message) throws IOException
	{
		System.out.println("Sending bcast "+message );
		
		channel.basicPublish( Commons.EXCHANGE_NAME, "", null, Commons.Serialize(message).getBytes());
	}

	public static void main(String[] argv) throws Exception {

		//System.out.println(" "+argv[0]);
		int myNo = Integer.parseInt(argv[0]);
		int totalNodes = Integer.parseInt(argv[2]);
		Sender obj = new Sender(myNo, argv[1], totalNodes);
		new Thread(obj).start();
		obj.sendInitMsg();

	}


}