package org.apache.cassandra.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.thrift.InvalidRequestException;
import java.util.Map;
import java.util.HashMap;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;

class RemoteClient implements Client{
	private static final Logger logger = LoggerFactory.getLogger(RemoteClient.class);
	private final String[] locations;
	private int currentPos = 0;
	private String currentLocation;
	private TSocket currentSocket;
	private Cassandra.Client currentClient;
	private final Configuration conf;
	private boolean isOpen = false;
	private final String keyspace;

	RemoteClient(String[] locations,String keyspace,Configuration conf ) 
	{
		this.locations = locations;
		this.keyspace = keyspace;
		this.conf = conf;
	}

	@Override
	public void open() throws IOException
	{
		//try and get a client
		borrow();
		isOpen = true;
	}

	@Override
	public boolean isOpen() 
	{
		return isOpen;
	}

	@Override
	public void close()
	{
		if (currentSocket !=null && currentSocket.isOpen())
		{
			currentSocket.close();
			currentSocket = null;
			currentClient = null;
		}
	}
	
	@Override
	public <T> T execute(Client.Command<T> command) throws IOException
	{
		if (currentClient ==null)
			borrow();

		while (true)
		{
			try{
				return command.execute(currentClient);
			}
			catch(TTransportException e)
			{
				logger.info("Error communicating with endpoint "+ currentLocation,e);
				//try to get a new client
				borrow();
			}
			catch(TimedOutException e)
			{
				logger.info("Timed out when attempting to fulfill request, trying again",e);
				//get a new client
				borrow();
			}
			catch (UnavailableException e)
			{
				/*
				 * UnavailableExceptions means that that *particular* cassandra node believes that
				 * it doesn't have enough replicas available to fulfill that request. However, that could
				 * be due to a small network partition that only affected that one node--it's important
				 * to try again on another machine and see if we can still see results with our desired 
				 * ConsistencyLevel
				 */
				logger.warn("UnavailableException thrown, location "+ currentLocation +" believes there "+
										"is a network partition",e);
				borrow();
			}
			catch (InvalidRequestException e)
			{
				//can't do anything--this is a configuration problem
				throw new IOException(e);
			}
			catch (TException e)
			{
				//generic exception, we don't know what to do with it, so we bomb out
				logger.error("Unexpected exception occurred, failing request",e);
				throw new IOException(e);
			}
		}
	}

	private void borrow() throws IOException
	{
		//make sure any currently open sockets get closed
		if (currentSocket !=null &&currentSocket.isOpen())
		{
			currentSocket.close();
		}

		for (int i=0;i<locations.length;i++)
		{
			int pos = currentPos%locations.length;
			currentPos++;
			currentLocation = locations[pos];
			try
			{
				int rpcPort = ConfigHelper.getInputRpcPort(conf);
				int rpcTimeout = ConfigHelper.getInputRpcTimeout(conf);
				currentSocket = new TSocket(currentLocation,rpcPort,rpcTimeout);
				TBinaryProtocol protocol = new TBinaryProtocol(new TFramedTransport(currentSocket));
				currentClient = new Cassandra.Client(protocol);
				currentSocket.open();

				currentClient.set_keyspace(keyspace);
				if (ConfigHelper.getInputKeyspaceUserName(conf) != null)
				{
	        Map<String, String> creds = new HashMap<String, String>();
	        creds.put(IAuthenticator.USERNAME_KEY, ConfigHelper.getInputKeyspaceUserName(conf));
	        creds.put(IAuthenticator.PASSWORD_KEY, ConfigHelper.getInputKeyspacePassword(conf));
	        AuthenticationRequest authRequest = new AuthenticationRequest(creds);
	        currentClient.login(authRequest);
				}
			}
			catch (InvalidRequestException e)
			{
				//caused by configuration error, bomb out
				logger.error("Configuration error",e);
				throw new IOException(e);
			}
			catch (AuthenticationException e)
			{
				//caused by authentication error, bomb out
				logger.error("Authentication error",e);
				throw new IOException(e);
			}
			catch (AuthorizationException e)
			{
				//caused by authentication error, bomb out
				logger.error("Authorization error",e);
				throw new IOException(e);
			}
			catch (TTransportException e)
			{
				logger.info("Unable to connect to endpoint "+currentLocation,e);	
			}
			catch(TException e)
			{
				//unexpected error, bombout
				logger.error("Unexpected error connecting to endpoint " +currentLocation,e);
				throw new IOException(e);
			}
		}
		throw new IOException("Unable to connect to any replicas");
	}
}
