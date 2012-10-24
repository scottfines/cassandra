package org.apache.cassandra.hadoop;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.*;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.ConfigurationException;
//import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.cassandra.thrift.InvalidRequestException;

public class ColumnFamilyRecordReader extends RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>>
    implements org.apache.hadoop.mapred.RecordReader<ByteBuffer, SortedMap<ByteBuffer, IColumn>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyRecordReader.class);

    public static final int CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT = 8192;

    private ColumnFamilySplit split;
    private RowIterator iter;
    private Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> currentRow;
    private SlicePredicate predicate;
    private boolean isEmptyPredicate;
    private int totalRowCount; // total number of rows to fetch
    private int batchSize; // fetch this many per batch
    private String cfName;
    private String keyspace;
		private Client client;
    private ConsistencyLevel consistencyLevel;
    private int keyBufferSize = 8192;
    private List<IndexExpression> filter;

    public ColumnFamilyRecordReader()
    {
        this(ColumnFamilyRecordReader.CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT);
    }

    public ColumnFamilyRecordReader(int keyBufferSize)
    {
        super();
        this.keyBufferSize = keyBufferSize;
    }

    public void close()
    {
			client.close();
			/*
        if (socket != null && socket.isOpen())
        {
            socket.close();
            socket = null;
            client = null;
        }
				*/
    }

    public ByteBuffer getCurrentKey()
    {
        return currentRow.left;
    }

    public SortedMap<ByteBuffer, IColumn> getCurrentValue()
    {
        return currentRow.right;
    }

    public float getProgress()
    {
        // TODO this is totally broken for wide rows
        // the progress is likely to be reported slightly off the actual but close enough
        float progress = ((float) iter.rowsRead() / totalRowCount);
        return progress > 1.0F ? 1.0F : progress;
    }

    static boolean isEmptyPredicate(SlicePredicate predicate)
    {
        if (predicate == null)
            return true;

        if (predicate.isSetColumn_names() && predicate.getSlice_range() == null)
            return false;

        if (predicate.getSlice_range() == null)
            return true;

        byte[] start = predicate.getSlice_range().getStart();
        if ((start != null) && (start.length > 0))
            return false;
            
        byte[] finish = predicate.getSlice_range().getFinish();
        if ((finish != null) && (finish.length > 0))
            return false;

        return true;
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = context.getConfiguration();
        KeyRange jobRange = ConfigHelper.getInputKeyRange(conf);
        filter = jobRange == null ? null : jobRange.row_filter;
        predicate = ConfigHelper.getInputSlicePredicate(conf);
        boolean widerows = ConfigHelper.getInputIsWide(conf);
        isEmptyPredicate = isEmptyPredicate(predicate);
        totalRowCount = ConfigHelper.getInputSplitSize(conf);
        batchSize = ConfigHelper.getRangeBatchSize(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getReadConsistencyLevel(conf));

        keyspace = ConfigHelper.getInputKeyspace(conf);

        try
        {
					if(client !=null && client.isOpen())
						return;

					client = ConfigHelper.isRemote(conf) ? new RemoteClient(split.getLocations(),keyspace,conf) :
																								 new LocalClient(split.getLocations(),keyspace,conf);

					client.open();
					/*
            // only need to connect once
            if (socket != null && socket.isOpen())
                return;

            // create connection using thrift
            String location = getLocation();
            socket = new TSocket(location, ConfigHelper.getInputRpcPort(conf));
            TTransport transport = ConfigHelper.getInputTransportFactory(conf).openTransport(socket);
            TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport);
            client = new Cassandra.Client(binaryProtocol);

            // log in
            client.set_keyspace(keyspace);
            if (ConfigHelper.getInputKeyspaceUserName(conf) != null)
            {
                Map<String, String> creds = new HashMap<String, String>();
                creds.put(IAuthenticator.USERNAME_KEY, ConfigHelper.getInputKeyspaceUserName(conf));
                creds.put(IAuthenticator.PASSWORD_KEY, ConfigHelper.getInputKeyspacePassword(conf));
                AuthenticationRequest authRequest = new AuthenticationRequest(creds);
                client.login(authRequest);
            }
						*/
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        iter = widerows ? new WideRowIterator() : new StaticRowIterator();
        logger.debug("created {}", iter);
    }

    public boolean nextKeyValue() throws IOException
    {
        if (!iter.hasNext())
            return false;
        currentRow = iter.next();
        return true;
    }

    private abstract class RowIterator extends AbstractIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>>
    {
        protected List<KeySlice> rows;
        protected int totalRead = 0;
        protected AbstractType<?> comparator;
        protected AbstractType<?> subComparator;
        protected IPartitioner partitioner;

        private RowIterator()
        {
            try
            {
							client.execute(new Client.Command<Void>(){
								@Override
								public Void execute(Cassandra.Client cassandraClient) throws TimedOutException,
																											UnavailableException,TException,InvalidRequestException
								{
									/*
									 * Nothing in what follows actually will throw a TimedOutException or UnavailableException,
									 * but may as well indicate that it could someday.
									 */
									try
									{
		                RowIterator.this.partitioner = 
																FBUtilities.newPartitioner(cassandraClient.describe_partitioner());
		
		                // Get the Keyspace metadata, then get the specific CF metadata
		                // in order to populate the sub/comparator.
		                KsDef ks_def = cassandraClient.describe_keyspace(keyspace);
		                List<String> cfnames = new ArrayList<String>();
		                for (CfDef cfd : ks_def.cf_defs)
		                    cfnames.add(cfd.name);
		                int idx = cfnames.indexOf(cfName);
		                CfDef cf_def = ks_def.cf_defs.get(idx);
		
		                RowIterator.this.comparator = TypeParser.parse(cf_def.comparator_type);
		                RowIterator.this.subComparator = cf_def.subcomparator_type == null ? null : 
																										TypeParser.parse(cf_def.subcomparator_type);
									}
									catch(ConfigurationException e)
									{
										throw new TException(e);
									}
									catch(NotFoundException e)
									{
										throw new TException(e);
									}
//									catch(SyntaxException e)
//									{
//										throw new TException(e);
//									}
									return null;
								}
							});
								/*
                partitioner = FBUtilities.newPartitioner(client.describe_partitioner());

                // Get the Keyspace metadata, then get the specific CF metadata
                // in order to populate the sub/comparator.
                KsDef ks_def = client.describe_keyspace(keyspace);
                List<String> cfnames = new ArrayList<String>();
                for (CfDef cfd : ks_def.cf_defs)
                    cfnames.add(cfd.name);
                int idx = cfnames.indexOf(cfName);
                CfDef cf_def = ks_def.cf_defs.get(idx);

                comparator = TypeParser.parse(cf_def.comparator_type);
                subComparator = cf_def.subcomparator_type == null ? null : TypeParser.parse(cf_def.subcomparator_type);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException("unable to load sub/comparator", e);
            }
            catch (TException e)
            {
                throw new RuntimeException("error communicating via Thrift", e);
            }
            catch (Exception e)
            {
                throw new RuntimeException("unable to load keyspace " + keyspace, e);
            }
						*/
						}
						catch (IOException e)
						{
							throw new RuntimeException("Unexpected error initializing RowIterator",e);
						}
        }

        /**
         * @return total number of rows read by this record reader
         */
        public int rowsRead()
        {
            return totalRead;
        }

        protected IColumn unthriftify(ColumnOrSuperColumn cosc)
        {
            if (cosc.counter_column != null)
                return unthriftifyCounter(cosc.counter_column);
            if (cosc.counter_super_column != null)
                return unthriftifySuperCounter(cosc.counter_super_column);
            if (cosc.super_column != null)
                return unthriftifySuper(cosc.super_column);
            assert cosc.column != null;
            return unthriftifySimple(cosc.column);
        }

        private IColumn unthriftifySuper(SuperColumn super_column)
        {
            org.apache.cassandra.db.SuperColumn sc = new org.apache.cassandra.db.SuperColumn(super_column.name, subComparator);
            for (Column column : super_column.columns)
            {
                sc.addColumn(unthriftifySimple(column));
            }
            return sc;
        }

        protected IColumn unthriftifySimple(Column column)
        {
            return new org.apache.cassandra.db.Column(column.name, column.value, column.timestamp);
        }

        private IColumn unthriftifyCounter(CounterColumn column)
        {
            //CounterColumns read the nodeID from the System table, so need the StorageService running and access
            //to cassandra.yaml. To avoid a Hadoop needing access to yaml return a regular Column.
            return new org.apache.cassandra.db.Column(column.name, ByteBufferUtil.bytes(column.value), 0);
        }

        private IColumn unthriftifySuperCounter(CounterSuperColumn superColumn)
        {
            org.apache.cassandra.db.SuperColumn sc = new org.apache.cassandra.db.SuperColumn(superColumn.name, subComparator);
            for (CounterColumn column : superColumn.columns)
                sc.addColumn(unthriftifyCounter(column));
            return sc;
        }
    }

    private class StaticRowIterator extends RowIterator
    {
        protected int i = 0;

        private void maybeInit()
        {
            // check if we need another batch
            if (rows != null && i < rows.size())
                return;

            String startToken;
            if (totalRead == 0)
            {
                // first request
                startToken = split.getStartToken();
            }
            else
            {
                startToken = partitioner.getTokenFactory().toString(partitioner.getToken(Iterables.getLast(rows).key));
                if (startToken.equals(split.getEndToken()))
                {
                    // reached end of the split
                    rows = null;
                    return;
                }
            }

            final KeyRange keyRange = new KeyRange(batchSize)
                                .setStart_token(startToken)
                                .setEnd_token(split.getEndToken())
                                .setRow_filter(filter);
            try
            {
								rows = client.execute(new Client.Command<List<KeySlice>>()
								{
									@Override
									public List<KeySlice> execute(Cassandra.Client cassandraClient) 
											throws TimedOutException,UnavailableException,InvalidRequestException,TException
									{
										return cassandraClient.get_range_slices(
																	new ColumnParent(cfName),predicate,keyRange,consistencyLevel);
									}
								});
						}
						catch (IOException e)
						{
							throw new RuntimeException("Unexpected error getting range slices",e);
						}
            //rows = client.get_range_slices(new ColumnParent(cfName), predicate, keyRange, consistencyLevel);

            // nothing new? reached the end
            if (rows.isEmpty())
            {
                rows = null;
                return;
            }

                // prepare for the next slice to be read
                KeySlice lastRow = rows.get(rows.size() - 1);
                ByteBuffer rowkey = lastRow.key;
                startToken = partitioner.getTokenFactory().toString(partitioner.getToken(rowkey));

                // remove ghosts when fetching all columns
                if (isEmptyPredicate)
            // remove ghosts when fetching all columns
            if (isEmptyPredicate)
            {
                Iterator<KeySlice> it = rows.iterator();
                KeySlice ks;
                do
                {
                    ks = it.next();
                    if (ks.getColumnsSize() == 0)
                    {
                        it.remove();
                    }
                } while (it.hasNext());

                // all ghosts, spooky
                if (rows.isEmpty())
                {
                    // maybeInit assumes it can get the start-with key from the rows collection, so add back the last
                    rows.add(ks);
                    maybeInit();
                    return;
                }
            }

            // reset to iterate through this new batch
            i = 0;
						/*
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
						*/
        }

        protected Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> computeNext()
        {
            maybeInit();
            if (rows == null)
                return endOfData();

            totalRead++;
            KeySlice ks = rows.get(i++);
            SortedMap<ByteBuffer, IColumn> map = new TreeMap<ByteBuffer, IColumn>(comparator);
            for (ColumnOrSuperColumn cosc : ks.columns)
            {
                IColumn column = unthriftify(cosc);
                map.put(column.name(), column);
            }
            return new Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>(ks.key, map);
//            return Pair.create(ks.key,map);
        }
    }

    private class WideRowIterator extends RowIterator
    {
        private PeekingIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>> wideColumns;
        private ByteBuffer lastColumn = ByteBufferUtil.EMPTY_BYTE_BUFFER;

        private void maybeInit()
        {
            if (wideColumns != null && wideColumns.hasNext())
                return;

            KeyRange keyRange;
//            ByteBuffer startColumn;
            if (totalRead == 0)
            {
                String startToken = split.getStartToken();
                keyRange = new KeyRange(batchSize)
                          .setStart_token(startToken)
                          .setEnd_token(split.getEndToken())
                          .setRow_filter(filter);
            }
            else
            {
                KeySlice lastRow = Iterables.getLast(rows);
                logger.debug("Starting with last-seen row {}", lastRow.key);
                keyRange = new KeyRange(batchSize)
                          .setStart_key(lastRow.key)
                          .setEnd_token(split.getEndToken())
                          .setRow_filter(filter);

            }

							rows = getRows(keyRange,lastColumn);
//                rows = client.get_paged_slice(cfName, keyRange, lastColumn, consistencyLevel);
                int n = 0;
                for (KeySlice row : rows)
                    n += row.columns.size();
                logger.debug("read {} columns in {} rows for {} starting with {}",
                             new Object[]{ n, rows.size(), keyRange, lastColumn });

                wideColumns = Iterators.peekingIterator(new WideColumnIterator(rows));
                if (wideColumns.hasNext() && wideColumns.peek().right.keySet().iterator().next().equals(lastColumn))
                    wideColumns.next();
                if (!wideColumns.hasNext())
                    rows = null;
						/*
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
						*/
        }
				private List<KeySlice> getRows(final KeyRange range, final ByteBuffer lastColumn) {
					try
					{
						return client.execute(new Client.Command<List<KeySlice>>()
							{
								@Override
								public List<KeySlice> execute(Cassandra.Client cassandraClient) 
									throws TimedOutException,UnavailableException,TException
								{
									try
									{
										return cassandraClient.get_paged_slice(cfName,range,
																													lastColumn,consistencyLevel);
									}
									catch(InvalidRequestException e)
									{
										throw new TException(e);
									}
								}
							});
					}
					catch(IOException e)
					{
						throw new RuntimeException("Unexpected error getting paged range slice",e);
					}
				}

        protected Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> computeNext()
        {
            maybeInit();
            if (rows == null)
                return endOfData();

            totalRead++;
            Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> next = wideColumns.next();
            lastColumn = next.right.values().iterator().next().name();
            return next;
        }

        private class WideColumnIterator extends AbstractIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>>>
        {
            private final Iterator<KeySlice> rows;
            private Iterator<ColumnOrSuperColumn> columns;
            public KeySlice currentRow;

            public WideColumnIterator(List<KeySlice> rows)
            {
                this.rows = rows.iterator();
                if (this.rows.hasNext())
                    nextRow();
                else
                    columns = Iterators.emptyIterator();
            }

            private void nextRow()
            {
                currentRow = rows.next();
                columns = currentRow.columns.iterator();
            }

            protected Pair<ByteBuffer, SortedMap<ByteBuffer, IColumn>> computeNext()
            {
                while (true)
                {
                    if (columns.hasNext())
                    {
                        ColumnOrSuperColumn cosc = columns.next();
                        IColumn column = unthriftify(cosc);
                        ImmutableSortedMap<ByteBuffer, IColumn> map = ImmutableSortedMap.of(column.name(), column);
                        return Pair.<ByteBuffer, SortedMap<ByteBuffer, IColumn>>create(currentRow.key, map);
                    }

                    if (!rows.hasNext())
                        return endOfData();

                    nextRow();
                }
            }
        }
    }

    // Because the old Hadoop API wants us to write to the key and value
    // and the new asks for them, we need to copy the output of the new API
    // to the old. Thus, expect a small performance hit.
    // And obviously this wouldn't work for wide rows. But since ColumnFamilyInputFormat
    // and ColumnFamilyRecordReader don't support them, it should be fine for now.
    public boolean next(ByteBuffer key, SortedMap<ByteBuffer, IColumn> value) throws IOException
    {
        if (this.nextKeyValue())
        {
            key.clear();
            key.put(this.getCurrentKey());
            key.flip();

            value.clear();
            value.putAll(this.getCurrentValue());

            return true;
        }
        return false;
    }

    public ByteBuffer createKey()
    {
        return ByteBuffer.wrap(new byte[this.keyBufferSize]);
    }

    public SortedMap<ByteBuffer, IColumn> createValue()
    {
        return new TreeMap<ByteBuffer, IColumn>();
    }

    public long getPos() throws IOException
    {
        return (long)iter.rowsRead();
    }
}
