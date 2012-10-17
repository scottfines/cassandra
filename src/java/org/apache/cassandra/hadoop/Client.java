package org.apache.cassandra.hadoop;

import org.apache.cassandra.thrift.Cassandra;
import java.io.IOException;
import org.apache.thrift.TException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.InvalidRequestException;

/**
 * Wraps out the logic around executing commands to Cassandra during a MapReduce job.
 *
 * This interface allows us to use either a Local Client (e.g. running TaskTrackers on the same nodes as 
 * Cassandra) or a Remote Client (e.g. running TaskTrackers on a different cluster).
 */
interface Client {

	/**
	 * The cassandra command to execute.
	 *
	 */
	interface Command<T> {
		T execute(Cassandra.Client client) throws TimedOutException,UnavailableException,
																													InvalidRequestException,TException;
	}

	/**
	 * Close any underlying Sockets/Clients which are still open.
	 */
	void close();

	/**
	 * @return true if the client has already been opened.
	 */
	boolean isOpen();

	/**
	 * Open the client and prepare for activity.
	 */
	void open() throws IOException;

	/**
	 * Execute the specific Command, dealing with faults in an implementation-specific manner.
	 */
	public <T> T execute(Command<T> command) throws IOException;
}
