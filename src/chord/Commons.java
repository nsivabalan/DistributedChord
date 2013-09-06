package chord;

import com.google.gson.Gson;

import chord.ChordMessage.ChordMsg;

public class Commons {

	public static final String EXCHANGE_NAME = "bcastQueue2";
	
	public static ChordMsg getInitMessage(int myNo, String qName)
	{
		InitMessage.InitMsg.Builder getMsgBuilder = InitMessage.InitMsg.newBuilder();
		getMsgBuilder.setMyno(myNo);
		getMsgBuilder.setQname(qName);
		InitMessage.InitMsg  getMsg = getMsgBuilder.build();
		ChordMessage.ChordMsg.Builder getChordMsgBuilder = ChordMessage.ChordMsg.newBuilder();
		getChordMsgBuilder.setInitMsg(getMsg);
		getChordMsgBuilder.setMsgType("InitMessage");
		ChordMessage.ChordMsg getChordMsg = getChordMsgBuilder.build();
		return getChordMsg;
	}
	
	
	public static ChordMsg getInitAckMessage(int myNo, String qName)
	{
		InitAckMessage.InitAckMsg.Builder getMsgBuilder = InitAckMessage.InitAckMsg.newBuilder();
		getMsgBuilder.setMyno(myNo);
		getMsgBuilder.setQname(qName);
		InitAckMessage.InitAckMsg  getMsg = getMsgBuilder.build();
		ChordMessage.ChordMsg.Builder getChordMsgBuilder = ChordMessage.ChordMsg.newBuilder();
		getChordMsgBuilder.setInitActMsg(getMsg);
		getChordMsgBuilder.setMsgType("InitAckMessage");
		ChordMessage.ChordMsg getChordMsg = getChordMsgBuilder.build();
		return getChordMsg;
	}
	
	
	public static ChordMsg getClientGetMessage(String key)
	{
		ClientGetMessage.ClientGetMsg.Builder getMsgBuilder = ClientGetMessage.ClientGetMsg.newBuilder();
		getMsgBuilder.setKey(key);
		ClientGetMessage.ClientGetMsg  getMsg = getMsgBuilder.build();
		ChordMessage.ChordMsg.Builder getChordMsgBuilder = ChordMessage.ChordMsg.newBuilder();
		getChordMsgBuilder.setClientGetMsg(getMsg);
		getChordMsgBuilder.setMsgType("ClientGetMessage");
		ChordMessage.ChordMsg getChordMsg = getChordMsgBuilder.build();
		return getChordMsg;
	}
	
	public static ChordMsg getClientSetMessage(String key, String value)
	{
		ClientSetMessage.ClientSetMsg.Builder getMsgBuilder = ClientSetMessage.ClientSetMsg.newBuilder();
		getMsgBuilder.setKey(key);
		getMsgBuilder.setValue(value);
		ClientSetMessage.ClientSetMsg  getMsg = getMsgBuilder.build();
		ChordMessage.ChordMsg.Builder getChordMsgBuilder = ChordMessage.ChordMsg.newBuilder();
		getChordMsgBuilder.setClientSetMsg(getMsg);
		getChordMsgBuilder.setMsgType("ClientSetMessage");
		ChordMessage.ChordMsg getChordMsg = getChordMsgBuilder.build();
		return getChordMsg;
	}
	
	public static ChordMsg getClientResponseMessage(String key, String value)
	{
		ClientResponseMessage.ClientResponseMsg.Builder getMsgBuilder = ClientResponseMessage.ClientResponseMsg.newBuilder();
		getMsgBuilder.setKey(key);
		getMsgBuilder.setValue(value);
		ClientResponseMessage.ClientResponseMsg  getMsg = getMsgBuilder.build();
		ChordMessage.ChordMsg.Builder getChordMsgBuilder = ChordMessage.ChordMsg.newBuilder();
		getChordMsgBuilder.setClientResponseMsg(getMsg);
		getChordMsgBuilder.setMsgType("ClientResponseMessage");
		ChordMessage.ChordMsg getChordMsg = getChordMsgBuilder.build();
		return getChordMsg;
	}
	
	//Static Functions
		public static <T> String Serialize(T message)
		{
			Gson gson = new Gson();
			return gson.toJson(message, message.getClass());
		}

		//TODO: Use this function instead of local deserialization function in RMQReceiver. 
		@SuppressWarnings("rawtypes")
		public static <T> T Deserialize(String json, Class className)
		{
			Gson gson = new Gson();
			return (T) gson.fromJson(json, className);
		}
	
	
}
