package org.apache.cassandra.hadoop;

import java.io.IOException;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import java.net.UnknownHostException;
import java.lang.AssertionError;
import java.util.Collections;

public class LocalClient implements Client{
	private Cassandra.Client client;
	private TSocket socket;
	private Configuration conf;
	private String location;
	private String keyspace;

	LocalClient(String[] locations,String keyspace,Configuration conf)
	{
		this.conf = conf;
		this.location = getLocation(locations);
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

  // we don't use endpointsnitch since we are trying to support hadoop nodes that are
  // not necessarily on Cassandra machines, too.  This should be adequate for single-DC clusters, at least.
  private String getLocation(String[] locations)
  {
      ArrayList<InetAddress> localAddresses = new ArrayList<InetAddress>();
      try
      {
          Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
          while (nets.hasMoreElements())
              localAddresses.addAll(Collections.list(nets.nextElement().getInetAddresses()));
      }
      catch (SocketException e)
      {
          throw new AssertionError(e);
      }

      for (InetAddress address : localAddresses)
      {
          for (String location : locations)
          {
              InetAddress locationAddress = null;
              try
              {
                  locationAddress = InetAddress.getByName(location);
              }
              catch (UnknownHostException e)
              {
                  throw new AssertionError(e);
              }
              if (address.equals(locationAddress))
              {
                  return location;
              }
          }
      }
      return locations[0];
  }
}
