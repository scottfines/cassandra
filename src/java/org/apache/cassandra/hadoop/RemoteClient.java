package org.apache.cassandra.hadoop;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.thrift.InvalidRequestException;
import java.util.Map;
import java.util.HashMap;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;

/**
 * Remote Client for Cassandra MR calls.
 * 
 * This allows one to make calls to external Cassandra systems which are tolerant to failing Cassandra
 * nodes. Instead of relying on cohabitating a TaskTracker and a Cassandra instance, this allows one
 * to run TaskTrackers on separate boxes and thus allows for bulk <em>move</em> operations, rather than
 * just bulk-processing operations.
 */
public class RemoteClient implements Client{
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
		org.apache.log4j.Logger underLogger = org.apache.log4j.Logger.getLogger(RemoteClient.class);
		underLogger.setLevel(org.apache.log4j.Level.DEBUG);
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
		
		int numTries = locations.length-1;
		while (numTries>=0)
		{
			numTries--;
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
				logger.error("Failing request due to poor configuration",e);
				throw new IOException(e);
			}
			catch (TException e)
			{
				//generic exception, we don't know what to do with it, so we bomb out
				logger.error("Unexpected exception occurred, failing request",e);
				throw new IOException(e);
			}
		}
		logger.error("Could not complete request successfully after {} tries, failing request",locations.length);
		throw new IOException(
							String.format("Could not complete request successfully after %d tries",locations.length));
	}

	private void borrow() throws IOException
	{
		//make sure any currently open sockets get closed
		if (currentSocket !=null &&currentSocket.isOpen())
		{
			logger.trace("Closing already open socket");
			currentSocket.close();
		}
		logger.info("Attempting to connect to any of ",locations);
		for (int i=0;i<locations.length;i++)
		{
			int pos = currentPos%locations.length;
			currentPos++;
			currentLocation = locations[pos];
			try
			{
				logger.trace("Attempting to connect to endpoint {}",currentLocation);
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
				logger.trace("Successfully connected to endpoint {}",currentLocation);
				return;
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
		throw new IOException("Unable to connect to any of replicas ["+StringUtils.join(locations,",")+"]");
	}
}
