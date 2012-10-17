package org.apache.cassandra.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TSocket;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.AuthenticationRequest;
import java.util.Map;
import java.util.HashMap;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.InvalidRequestException;

class LocalClient implements Client{
	private Cassandra.Client client;
	private TSocket socket;
	private Configuration conf;
	private String location;
	private String keyspace;

	LocalClient(String location,String keyspace,Configuration conf)
	{
		this.conf = conf;
		this.location = location;
		this.keyspace = keyspace;
	}
	
	@Override
	public void close () 
	{
		if (isOpen())
		{
			socket.close();
			socket = null;
			client = null;
		}
	}

	@Override
	public boolean isOpen ()
	{
		return socket !=null && socket.isOpen();
	}

	@Override
	public void open() throws IOException
	{
		int rpcPort = ConfigHelper.getInputRpcPort(conf);
		try
		{
			socket = new TSocket(location,rpcPort);
			TTransport transport = ConfigHelper.getInputTransportFactory(conf).openTransport(socket);
			TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport);
			client = new Cassandra.Client(binaryProtocol);

			//log in
			client.set_keyspace(keyspace);
			if (ConfigHelper.getInputKeyspaceUserName(conf) !=null)
			{
        Map<String, String> creds = new HashMap<String, String>();
        creds.put(IAuthenticator.USERNAME_KEY, ConfigHelper.getInputKeyspaceUserName(conf));
        creds.put(IAuthenticator.PASSWORD_KEY, ConfigHelper.getInputKeyspacePassword(conf));
        AuthenticationRequest authRequest = new AuthenticationRequest(creds);
        client.login(authRequest);
			}
		}
		catch (Exception e)
		{
			throw new IOException(e);
		}
	}
	
	@Override
	public <T> T execute(Client.Command<T> command) throws IOException
	{
		try
		{
			return command.execute(client);
		}
		catch (TimedOutException e)
		{
			throw new IOException(e);
		}
		catch (UnavailableException e)
		{
			throw new IOException(e);
		}
		catch (InvalidRequestException e)
		{
			throw new IOException(e);
		}
		catch (TException e)
		{
			throw new IOException(e);
		}
	}
}
