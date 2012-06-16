/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;


import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

import org.hypertable.thriftgen.*;
import org.hypertable.thrift.*;

import org.apache.thrift.TException;

/**
 * Hypertable client for YCSB framework
 */
public class HypertableClient extends com.yahoo.ycsb.DB
{
    public boolean _debug=false;
    
    public ThriftClient connection;
    public long ns;

    public String _columnFamily="";

    public static final int Ok=0;
    public static final int ServerError=-1;
    public static final int HttpError=-2;
    public static final int NoMatchingRecord=-3;

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException
    {
        if ( (getProperties().getProperty("debug")!=null) &&
                (getProperties().getProperty("debug").compareTo("true")==0) )
        {
            _debug=true;
        }
    	
    	//TODO: allow dyamic namespace specification?
    	try{
    		connection = ThriftClient.create("localhost", 38080);
    		
    		if(!connection.namespace_exists("/ycsb")){
    			connection.namespace_create("/ycsb");
    		}	
    		ns = connection.open_namespace("/ycsb");
    	}catch(ClientException e){
    		throw new DBException("Could not open namespace:\n"+e.message);
    	}catch(TException e){
    		throw new DBException("Could not open namespace");
    	}
    		
        
        _columnFamily = getProperties().getProperty("columnfamily");
        if (_columnFamily == null)
        {
            System.err.println("Error, must specify a columnfamily for Hypertable table");
            throw new DBException("No columnfamily specified");
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException
    {
    	try{
    		connection.namespace_close(ns);
    	}catch(ClientException e){
    		throw new DBException("Could not close namespace:\n"+e.message);
    	}catch(TException e){
    		throw new DBException("Could not close namespace");
    	}
    }
    
    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result)
    {
    	//SELECT _column_family:field[i] FROM table WHERE ROW=key MAX_VERSIONS 1;

    	if(_debug){
    		System.out.println("Doing read from Hypertable columnfamily "+_columnFamily);
    		System.out.println("Doing read for key: "+key);
    	}

    	List<Cell> values;
    	
    	//OPT: Get row then parse instead of requesting each individual field?

    	if(null != fields){
    		values = new ArrayList<Cell>();
    		for(String column : fields){
    			if(_debug)
    				System.out.println("retrieving column: "+column);
    			try{
    				Cell nextCol = new Cell();
    				nextCol.key = new Key();
    				nextCol.value = connection.get_cell(ns, table, key, _columnFamily+":"+column);
    				nextCol.key.column_qualifier = column;
    				values.add(nextCol);
    			}catch(ClientException e){
    				if(_debug){
    	        		System.err.println("Error doing read: "+e.message);
    	        	}
    				return ServerError;
    			}catch(TException e){
    				if(_debug)
    					System.err.println("Error doing read");
    				return ServerError;
    			}
    		}
    	}else{
    		try{
    			values = connection.get_row(ns, table, key);
    		}catch(ClientException e){
    			if(_debug){
            		System.err.println("Error doing read: "+e.message);
            	}
    			return ServerError;
    		}catch(TException e){
    			if(_debug)
    				System.err.println("Error doing read");
    			return ServerError;
    		}
    	}
    	
    	for(Cell nextValue : values){
    	    if(_debug){
    	    	System.out.println("Result for field: "+nextValue.key.column_qualifier+
    	    			" is: "+nextValue.value.toString());
    	    }
    	    if(nextValue.value.position() != nextValue.value.limit())
    	    	result.put(nextValue.key.column_qualifier, 
    	    			new ByteArrayByteIterator(nextValue.value.array(), 
    	    					nextValue.value.position(), nextValue.value.limit()-nextValue.value.position()));
    	}

    	return Ok;
    }
    
    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
    {
//    	final int largeDataReturn = 2000; //CHECK: What qualifies large data?

    	//SELECT _columnFamily:fields FROM table WHERE (ROW > startkey) LIMIT recordcount MAX_VERSIONS 1;

    	//TODO: sanitize inputs
    	String search = "SELECT ";
    	if(null == fields)
    		search+= "*";
    	else{
    		boolean first = true;
    		for(String field:fields){
    			if(!first)
    				search+= ", ";
    			first = false;
    			search+= _columnFamily+":"+field;
    		}
    	}
    	search+= " FROM ";
    	search+= table;
    	search+= " WHERE (ROW >= '";
    	search+= startkey;
    	search+= "') LIMIT ";
    	search+= Integer.toString(recordcount);
    	search+= " MAX_VERSIONS 1";

    	//OPT: use a scanner instead of buffered operation?
    	
//    	if(recordcount > largeDataReturn){
//    		long sc;
//    		try{
//    			sc = connection.hql_exec(ns, search, false, true).getScanner();
//    			
//    			//TODO: check that the row contents are only the specified columns 
//    			//CHECK: [style] split up/unify try statement?
//    			String lastRowName = null;
//    			boolean endReached = false;
//    			while(result.size() < recordcount && !endReached){
//    				//CHECK: lastRowName persists between iterations as I want it to?
//    				List<Cell> values = connection.scanner_get_cells(sc);
//    				if(values.size() >0 )
//    					parseCellList(values, result, lastRowName);
//    				else
//    					endReached = true;  //CHECK: can replace with break statement?
//    			}
//    			
//    			connection.close_scanner(sc);
//    		}catch(ClientException e){
//    			if(_debug){
//            		System.err.println("Error doing scan: "+e.message);
//            	}
//    			return ServerError;
//    		}catch(TException e){
//    			if(_debug)
//    				System.err.println("Error doing scan");
//    			return ServerError;
//    		}
//    	}else{
    		try{
    			parseCellList(connection.hql_query(ns, search).getCells(), result);
    		}catch(ClientException e){
    			if(_debug){
            		System.err.println("Error doing scan: "+e.message);
            	}
    			return ServerError;
    		}catch(TException e){
    			if(_debug)
    				System.err.println("Error doing scan");
    			return ServerError;
    		}
//    	}
    	
    	return Ok;
    }

    /**
     * Helper function for scan. Parses a List<Cell> into the right format for scan's results Vector
     *
     * @param values A list of Cells containing table values to be entered into result
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @param rowName Contains the name of the row corresponding for the pairs in result.lastElement()
     */
    public void parseCellList(List<Cell> values, Vector<HashMap<String,ByteIterator>> result, String rowName){
    	for(Cell value:values){
    		if(0 == result.size() || !value.key.getRow().equals(rowName)){
    			result.add(new HashMap<String,ByteIterator>());
    			rowName = value.key.getRow();
    		}
    		result.lastElement().put(value.key.column_qualifier, 
    				new ByteArrayByteIterator(value.value.array(), 
    						value.value.position(), value.value.limit()-value.value.position()));
    		if(_debug){
    	    	System.out.println("Result for field: "+value.key.column_qualifier+
    	    			" is: "+value.value.toString());
    		}
    	}
    }

    public void parseCellList(List<Cell> values, Vector<HashMap<String,ByteIterator>> result){
    	parseCellList(values, result, null);
    }


    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int update(String table, String key, HashMap<String,ByteIterator> values)
    {
    	return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int insert(String table, String key, HashMap<String,ByteIterator> values)
    {
    	//TODO: create the table if none exists?
    	
    	if (_debug) {
    		System.out.println("Setting up put for key: "+key);
    	}
    	
    	//INSERT INTO table VALUES (key, _column_family:entry,getKey(), entry.getValue()), (...);

    	List<Cell> cells = new ArrayList<Cell>();
        for(Map.Entry<String, ByteIterator> entry : values.entrySet()){
        	if(_debug){
        		System.out.println("Adding field/value " + entry.getKey() + "/" +
        				entry.getValue() + " to put request");
        	}
        	Cell nextInsert = new Cell();
        	nextInsert.key = new Key();
        	nextInsert.value = ByteBuffer.wrap(entry.getValue().toArray());
        	nextInsert.key.row = key;
        	nextInsert.key.column_family = _columnFamily;
        	nextInsert.key.column_qualifier = entry.getKey();;
        	nextInsert.key.flag = KeyFlag.INSERT;
        	cells.add(nextInsert);
        }
        
        try{
        	connection.set_cells(ns, table, cells);
        }catch(ClientException e){
        	if(_debug){
        		System.err.println("Error doing set: "+e.message);
        	}
        	return ServerError;
        }catch(TException e){
        	if(_debug)
        		System.err.println("Error doing set");
        	return ServerError;
        }
    	
        return Ok;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int delete(String table, String key)
    {
    	//DELETE * FROM table WHERE ROW=key;
    	
        if(_debug){
        	System.out.println("Doing delete for key: "+key);
        }
        
        Cell entry = new Cell();
        entry.key = new Key();
        entry.key.row = key;
        entry.key.flag = KeyFlag.DELETE_ROW;
        entry.key.column_family = _columnFamily;  //necessary?
        
        try{
        	connection.set_cell(ns, table, entry);
        }catch(ClientException e){
        	if(_debug){
        		System.err.println("Error doing delete: "+e.message);
        	}
        	return ServerError;        	
        }catch(TException e){
        	if(_debug)
        		System.err.println("Error doing delete");
        	return ServerError;
        }
      
        return Ok;
    }

    public static void main(String[] args)
    {
        if (args.length!=3)
        {
            System.out.println("Please specify a threadcount, columnfamily and operation count");
            System.exit(0);
        }

        final int keyspace=10000; //120000000;

        final int threadcount=Integer.parseInt(args[0]);

        final String columnfamily=args[1];


        final int opcount=Integer.parseInt(args[2])/threadcount;

        Vector<Thread> allthreads=new Vector<Thread>();

        for (int i=0; i<threadcount; i++)
        {
            Thread t=new Thread()
            {
                public void run()
                {
                    try
                    {
                        Random random=new Random();

                        HypertableClient cli=new HypertableClient();

                        Properties props=new Properties();
                        props.setProperty("columnfamily",columnfamily);
                        props.setProperty("debug","true"); 
                        cli.setProperties(props);

                        cli.init();

                        long accum=0;

                        for (int i=0; i<opcount; i++)
                        {
                            int keynum=random.nextInt(keyspace);
                            String key="user"+keynum;
                            long st=System.currentTimeMillis();
                            int rescode;

                            HashSet<String> scanFields = new HashSet<String>();
                            scanFields.add("field1");
                            scanFields.add("field3");
                            Vector<HashMap<String,ByteIterator>> scanResults = new Vector<HashMap<String,ByteIterator>>();
                            rescode = cli.scan("table1","user2",20,null,scanResults);

                            long en=System.currentTimeMillis();

                            accum+=(en-st);

                            if (rescode!=Ok)
                            {
                                System.out.println("Error "+rescode+" for "+key);
                            }

                            if (i%1==0)  //CHECK: won't this execute for every?
                            {
                                System.out.println(i+" operations, average latency: "+(((double)accum)/((double)i)));
                            }
                        }

                        //System.out.println("Average latency: "+(((double)accum)/((double)opcount)));
                        //System.out.println("Average get latency: "+(((double)cli.TotalGetTime)/((double)cli.TotalGetOps)));
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            };
            allthreads.add(t);
        }

        long st=System.currentTimeMillis();
        for (Thread t: allthreads)
        {
            t.start();
        }

        for (Thread t: allthreads)
        {
            try
            {
                t.join();
            }
            catch (InterruptedException e)
            {
            }
        }
        long en=System.currentTimeMillis();

        System.out.println("Throughput: "+((1000.0)*(((double)(opcount*threadcount))/((double)(en-st))))+" ops/sec");

    }
}

/* For customized vim control
 * set autoindent
 * set si
 * set shiftwidth=4
*/

