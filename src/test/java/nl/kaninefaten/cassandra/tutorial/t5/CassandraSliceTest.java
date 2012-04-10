package nl.kaninefaten.cassandra.tutorial.t5;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassandraSliceTest  {

	/** Serializers */
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	private static IntegerSerializer integerSerializer = IntegerSerializer.get();
    private static ObjectSerializer objectSerializer= ObjectSerializer.get();
	private static final BytesArraySerializer byteArraySerializer = BytesArraySerializer.get();

	
	/** Name of test cluster*/
	public static String clusterName = "TestCluster";
	
	/** Name and port of test cluster*/
	public static String host = "localhost:9171";
	
	/** Name of key space to create*/
	public static String keyspaceName = "keySpaceName";
	
	public static Keyspace keyspace = null;
	
	/** Name of the column family*/
	public static String columnFamilyName = "AColumnFamily";
	
	/** Cluster to talk to*/
	private static Cluster cluster = null;
	
	
	// Before these unit tests are called initialize the junit class with this server
	@BeforeClass
	public static void start() throws Throwable {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();

		// Gets the thrift client with name an host
		cluster = HFactory.getOrCreateCluster(clusterName, host);
		// Creates the keyspace in Cassandra
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, 1, null);
		cluster.addKeyspace(newKeyspace);
		// Keyspace to work with
		// A keyspace is like user or schema in Oracle
		keyspace = HFactory.createKeyspace(keyspaceName, cluster);	
	}

	// Break down the server when unit tests are executed.
	@AfterClass
	public static void stop(){
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	
	// Creates the ColumnFamily in Cassandra
	@Test
	public void testCreateOndexedColumnFamily() {
		// Create
		try {
			BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
			columnFamilyDefinition.setKeyspaceName(keyspaceName);
			columnFamilyDefinition.setName(columnFamilyName);
			columnFamilyDefinition.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());


			BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
			columnDefinition.setName(StringSerializer.get().toByteBuffer("creationTime"));
			columnDefinition.setValidationClass(ComparatorType.LONGTYPE.getClassName());
			columnFamilyDefinition.addColumnDefinition(columnDefinition);

			
			BasicColumnDefinition bcd = new BasicColumnDefinition(); 
			bcd.setName(StringSerializer.get().toByteBuffer("firstName"));
			bcd.setIndexName("firstName");
			bcd.setValidationClass("org.apache.cassandra.db.marshal.UTF8Type"); 
			bcd.setIndexType(ColumnIndexType.KEYS); 

			columnFamilyDefinition.addColumnDefinition(bcd);
			
			cluster.addColumnFamily(columnFamilyDefinition);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.toString());
		}
	}

	@Test
	public void testInsertTestDataSet() {
		CassandraDatasetLoader loader =  new CassandraDatasetLoader();
		try {
			loader.iterator(CassandraDatasetCreator.datasetFileHandler());
		} catch (IOException e) {
			fail(e.toString());
		}
		for (ExampleModelObject object : CassandraDatasetLoader.getExampleModelObjectList()){
			Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
			mutator.addInsertion(object.getKey(), columnFamilyName, HFactory.createStringColumn("firstName", object.getFirstName()));
			mutator.addInsertion(object.getKey(), columnFamilyName, HFactory.createStringColumn("middleName", object.getMiddleName()));
			mutator.addInsertion(object.getKey(), columnFamilyName, HFactory.createStringColumn("lastName", object.getLastName()));
			mutator.addInsertion(object.getKey(), columnFamilyName, HFactory.createStringColumn("emailAdress", object.getEmailAdress()));
			mutator.addInsertion(object.getKey(), columnFamilyName, HFactory.createStringColumn("password", object.getPassword()));
			mutator.addInsertion(object.getKey(), columnFamilyName, HFactory.createColumn("loginFailureCount", object.getLoginFailureCount(), stringSerializer, integerSerializer));
			mutator.addInsertion(object.getKey(), columnFamilyName, HFactory.createColumn("creationTime", object.getCreationTime(), stringSerializer , longSerializer));
			mutator.addInsertion(object.getKey(), columnFamilyName, HFactory.createColumn("updateTime", object.getUpdateTime(), stringSerializer, longSerializer));
			mutator.execute();
			
		}
		// Create
	}

	
	// Reads the row
	@Test
	public void testReadRow() {
		
		for (ExampleModelObject object : CassandraDatasetLoader.getExampleModelObjectList()){
			// Create query to fetch a result
			SliceQuery<String, String, Object> result = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, objectSerializer);
			result.setColumnFamily(columnFamilyName);
			result.setKey(object.getKey());	
			
			// To order the result
			// Note there is no ordering Cassandra
			String [] columnNames = new String[]{"firstName","middleName","lastName","emailAdress" , "password" , "loginFailureCount" , "creationTime" , "updateTime" };
			result.setColumnNames(columnNames);
			
			QueryResult<ColumnSlice<String, Object>> columnSlice = result.execute();
			
			if (columnSlice.get().getColumns().isEmpty()) {
				fail("Could not find created row");
			}
			
//			String value1 = (String)columnSlice.get().getColumnByName("firstName").getValue();
//			String value2 = (String)columnSlice.get().getColumnByName("middleName").getValue();
//			String value3 = (String)columnSlice.get().getColumnByName("lastName").getValue();
//			String value4 = (String)columnSlice.get().getColumnByName("emailAdress").getValue();
//			String value5 = (String)columnSlice.get().getColumnByName("password").getValue();
			
			ByteBuffer buffer = columnSlice.get().getColumnByName("loginFailureCount").getValueBytes();
			if (buffer != null){
				Integer i = integerSerializer.fromByteBuffer(buffer);
				System.out.println(i);
			}
			
//			Serializer serializer = columnSlice.get().getColumnByName("loginFailureCount").getValueSerializer();
//			ByteBuffer buffer = columnSlice.get().getColumnByName("loginFailureCount").getValueBytes();
//			
//			serializer.fromByteBuffer(byteBuffer)

//			System.out.println(buffer.asIntBuffer().toString());
//			Integer value6 = (Integer)
//			Long value7 = (Long)columnSlice.get().getColumnByName("creationTime").getValue();
//			Long value8 = (Long)columnSlice.get().getColumnByName("updateTime").getValue();
			
			
			//System.out.println( value6 );
//			System.out.println(value1 + ":" + value2 + ":" + value3 + ":" + value4 + ":" + value5 + ":" + value6 + ":" + value7 + ":" + value8);
//			Assert.assertEquals("Values must match",object.getFirstName(),value1);
//			Assert.assertEquals("Values must match",object.getMiddleName(),value2);
//			Assert.assertEquals("Values must match",object.getLastName(),value3);
//			Assert.assertEquals("Values must match",object.getEmailAdress(),value4);
			
			
		}
		

	}
	
	@Test
	public void sliceStringEqualsTest()
	{

		try{
		
        IndexedSlicesQuery<String, String, byte[]> indexedSlicesQuery = 
                HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, byteArraySerializer);

        indexedSlicesQuery.setColumnFamily(columnFamilyName);
        indexedSlicesQuery.setColumnNames("firstName","middleName","lastName");
        indexedSlicesQuery.addEqualsExpression("firstName", stringSerializer.toBytes("firstname10"));
//        indexedSlicesQuery.addEqualsExpression("city", stringSerializer.toBytes("Austin"));
        indexedSlicesQuery.addLteExpression("creationTime", longSerializer.toBytes(System.currentTimeMillis()));
        QueryResult<OrderedRows<String, String, byte[]>> result = indexedSlicesQuery.execute();
            
        String city = stringSerializer.fromBytes(result.get().iterator().next().getColumnSlice().getColumnByName("firstName").getValue());
        System.out.println(city);
		}catch (Throwable t){
			t.printStackTrace();
		}
	
	
	}
	
	@Test
	public void sliceLongLteTest()
	{

		try{
		
        IndexedSlicesQuery<String, String, byte[]> indexedSlicesQuery = 
                HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, byteArraySerializer);

        indexedSlicesQuery.setColumnFamily(columnFamilyName);
        indexedSlicesQuery.setColumnNames("firstName","middleName","lastName");
        indexedSlicesQuery.addEqualsExpression("firstName", stringSerializer.toBytes("firstname10"));
//        indexedSlicesQuery.addEqualsExpression("city", stringSerializer.toBytes("Austin"));
        indexedSlicesQuery.addLteExpression("creationTime", longSerializer.toBytes(System.currentTimeMillis()));
        QueryResult<OrderedRows<String, String, byte[]>> result = indexedSlicesQuery.execute();
            
        String city = stringSerializer.fromBytes(result.get().iterator().next().getColumnSlice().getColumnByName("firstName").getValue());
        System.out.println(city);
		}catch (Throwable t){
			t.printStackTrace();
		}
	
	
	}
	
	
	
}
