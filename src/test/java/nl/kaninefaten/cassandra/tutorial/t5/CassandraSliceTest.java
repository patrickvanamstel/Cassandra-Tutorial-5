package nl.kaninefaten.cassandra.tutorial.t5;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import junit.framework.Assert;

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
import me.prettyprint.hector.api.beans.Row;
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

/**
 * Secondary index test cases.
 * <p>
 * This test case is very verbose on purpose.
 * No magic mappings of code is done.
 * All code needed to map an query are in this test class.
 * <p>
 * Test cases need to be executed in order to work.
 * 
 * @author Patrick van Amstel
 * @date 2012 04 10
 *
 */
public class CassandraSliceTest  {

	/** Serializers */
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	private static IntegerSerializer integerSerializer = IntegerSerializer.get();
    private static ObjectSerializer objectSerializer= ObjectSerializer.get();
	private static final BytesArraySerializer byteArraySerializer = BytesArraySerializer.get();

	
	/** Name of test cluster*/
	public static String _clusterName = "TestCluster";
	
	/** Name and port of test cluster*/
	public static String _host = "localhost:9171";
	
	/** Name of key space to create*/
	public static String _keyspaceName = "keySpaceName";
	
	public static Keyspace _keyspace = null;
	
	/** Name of the column family*/
	public static String _columnFamilyName = "AColumnFamily";
	
	/** Cluster to talk to*/
	private static Cluster _cluster = null;
	
	// Column names
	private static String FIRSTNAME_COLUMN_NAME = "firstName";
	private static String MIDDLENAME_COLUMN_NAME 	="middleName";
	private static String LASTNAME_COLUMN_NAME 	="lastName";
	private static String EMAILADDRESS_COLUMN_NAME 	="emailAdress";
	private static String LOGINNAME_COLUMN_NAME 	="loginName";
	private static String PASSWORD_COLUMN_NAME 	="password";
	private static String LOGINFAILURE_COLUMN_NAME 	="loginFailureCount";
	private static String CREATIONTIME_COLUMN_NAME 	="creationTime";
	private static String UPDATETIME_COLUMN_NAME 	="updateTime";	

	
	
	
	// Before these unit tests are called initialize the junit class with this server
	@BeforeClass
	public static void start() throws Throwable {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();

		// Gets the thrift client with name an host
		_cluster = HFactory.getOrCreateCluster(_clusterName, _host);
		// Creates the keyspace in Cassandra
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(_keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, 1, null);
		_cluster.addKeyspace(newKeyspace);
		// Keyspace to work with
		// A keyspace is like user or schema in Oracle
		_keyspace = HFactory.createKeyspace(_keyspaceName, _cluster);	
	}

	// Break down the server when unit tests are executed.
	@AfterClass
	public static void stop(){
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	
	// Creates the ColumnFamily in Cassandra
	// Note the creation of indexes here.
	// In the secondary columns slices only columns can ben queried which are in here.
	@Test
	public void testCreateOndexedColumnFamily() {
		try {
			// Make a column family
			BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
			columnFamilyDefinition.setKeyspaceName(_keyspaceName);
			columnFamilyDefinition.setName(_columnFamilyName);
			columnFamilyDefinition.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());

			// Create a column that can be queried by a secondary slice
			BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
			columnDefinition.setName(StringSerializer.get().toByteBuffer(CREATIONTIME_COLUMN_NAME));
			columnDefinition.setValidationClass(ComparatorType.LONGTYPE.getClassName());
			columnFamilyDefinition.addColumnDefinition(columnDefinition);

			// Create a column that can be queried by a secondary slice
			BasicColumnDefinition bcd = new BasicColumnDefinition(); 
			bcd.setName(StringSerializer.get().toByteBuffer(FIRSTNAME_COLUMN_NAME));
			bcd.setIndexName(FIRSTNAME_COLUMN_NAME);
			bcd.setValidationClass("org.apache.cassandra.db.marshal.UTF8Type"); 
			bcd.setIndexType(ColumnIndexType.KEYS); 

			columnFamilyDefinition.addColumnDefinition(bcd);
			
			// Persist or execute update on cassandra
			_cluster.addColumnFamily(columnFamilyDefinition);
		} catch (Throwable t) {
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

		// Iterate over the created test set
		for (ExampleModelObject object : CassandraDatasetLoader.getExampleModelObjectList()){
			Mutator<String> mutator = HFactory.createMutator(_keyspace, stringSerializer);
			mutator.addInsertion(object.getKey(), _columnFamilyName, HFactory.createStringColumn(FIRSTNAME_COLUMN_NAME, object.getFirstName()));
			mutator.addInsertion(object.getKey(), _columnFamilyName, HFactory.createStringColumn(MIDDLENAME_COLUMN_NAME, object.getMiddleName()));
			mutator.addInsertion(object.getKey(), _columnFamilyName, HFactory.createStringColumn(LASTNAME_COLUMN_NAME, object.getLastName()));
			mutator.addInsertion(object.getKey(), _columnFamilyName, HFactory.createStringColumn(EMAILADDRESS_COLUMN_NAME, object.getEmailAdress()));
			mutator.addInsertion(object.getKey(), _columnFamilyName, HFactory.createStringColumn(LOGINNAME_COLUMN_NAME, object.getLoginName()));
			mutator.addInsertion(object.getKey(), _columnFamilyName, HFactory.createStringColumn(PASSWORD_COLUMN_NAME, object.getPassword()));
			
			// The first serializer is for the column name the second one is for the column value
			mutator.addInsertion(object.getKey(), _columnFamilyName, HFactory.createColumn(LOGINFAILURE_COLUMN_NAME, object.getLoginFailureCount(), stringSerializer, integerSerializer));
			mutator.addInsertion(object.getKey(), _columnFamilyName, HFactory.createColumn(CREATIONTIME_COLUMN_NAME, object.getCreationTime(), stringSerializer , longSerializer));
			mutator.addInsertion(object.getKey(), _columnFamilyName, HFactory.createColumn(UPDATETIME_COLUMN_NAME, object.getUpdateTime(), stringSerializer, longSerializer));
			mutator.execute();
		}
		// Create
	}
	

	private String toStringFromBuffer(ByteBuffer buffer){
		if (buffer == null){
			return null;
		}
		return stringSerializer.fromByteBuffer(buffer);
	}
	
	private Integer toIntegerFromBuffer(ByteBuffer buffer){
		if (buffer == null){
			return null;
		}
		return integerSerializer.fromByteBuffer(buffer);
		
	}
	
	private Long toLongFromBuffer(ByteBuffer buffer){
		if (buffer == null){
			return null;
		}
		return longSerializer.fromByteBuffer(buffer);
	}
	
	// Reads the row
	@Test
	public void testReadRow() {
		
		// Iterate over all keys.
		for (ExampleModelObject object : CassandraDatasetLoader.getExampleModelObjectList()){
			// Create query to fetch a result
			SliceQuery<String, String, Object> result = HFactory.createSliceQuery(_keyspace, stringSerializer, stringSerializer, objectSerializer);

			result.setColumnFamily(_columnFamilyName);
			result.setKey(object.getKey());	
			// To order the result
			// Note there is no ordering Cassandra
			String [] columnNames = new String[]{
					FIRSTNAME_COLUMN_NAME,
					MIDDLENAME_COLUMN_NAME,
					LASTNAME_COLUMN_NAME,
					EMAILADDRESS_COLUMN_NAME ,
					LOGINNAME_COLUMN_NAME,
					PASSWORD_COLUMN_NAME , 
					LOGINFAILURE_COLUMN_NAME , 
					CREATIONTIME_COLUMN_NAME , 
					UPDATETIME_COLUMN_NAME };
			result.setColumnNames(columnNames);
			
			QueryResult<ColumnSlice<String, Object>> columnSlice = result.execute();
			
			if (columnSlice.get().getColumns().isEmpty()) {
				fail("Could not find created row");
			}
			
			
			String firstName = toStringFromBuffer(columnSlice.get().getColumnByName(FIRSTNAME_COLUMN_NAME).getValueBytes());
			String middleName = toStringFromBuffer(columnSlice.get().getColumnByName(MIDDLENAME_COLUMN_NAME).getValueBytes());
			String lastName = toStringFromBuffer(columnSlice.get().getColumnByName(LASTNAME_COLUMN_NAME).getValueBytes());
			String emailAddress = toStringFromBuffer(columnSlice.get().getColumnByName(EMAILADDRESS_COLUMN_NAME).getValueBytes());
			String loginName = toStringFromBuffer(columnSlice.get().getColumnByName(LOGINNAME_COLUMN_NAME).getValueBytes());
			String password = toStringFromBuffer(columnSlice.get().getColumnByName(PASSWORD_COLUMN_NAME).getValueBytes());
			Integer loginFailure = toIntegerFromBuffer(columnSlice.get().getColumnByName(LOGINFAILURE_COLUMN_NAME).getValueBytes());
			Long creationTime = toLongFromBuffer(columnSlice.get().getColumnByName(CREATIONTIME_COLUMN_NAME).getValueBytes());
			Long updateTime = toLongFromBuffer(columnSlice.get().getColumnByName(UPDATETIME_COLUMN_NAME).getValueBytes());
			
			ExampleModelObject exampleModelObjectFromCassandra = new ExampleModelObject();
			exampleModelObjectFromCassandra.setFirstName(firstName);
			exampleModelObjectFromCassandra.setMiddleName(middleName);
			exampleModelObjectFromCassandra.setLastName(lastName);
			exampleModelObjectFromCassandra.setEmailAdress(emailAddress);
			exampleModelObjectFromCassandra.setLoginName(loginName);
			exampleModelObjectFromCassandra.setPassword(password);
			exampleModelObjectFromCassandra.setLoginFailureCount(loginFailure);
			exampleModelObjectFromCassandra.setCreationTime(creationTime);
			exampleModelObjectFromCassandra.setUpdateTime(updateTime);
			// Note this is a bit weird.
			exampleModelObjectFromCassandra.setKey(object.getKey());
			
			Assert.assertEquals("Values must match",object,exampleModelObjectFromCassandra);
		}
	}

	// Finally a simple query to execute
	@Test
	public void sliceStringEqualsTest() {

		try {
			// Create the query object
			IndexedSlicesQuery<String, String, byte[]> indexedSlicesQuery = HFactory
					.createIndexedSlicesQuery(_keyspace, stringSerializer,
							stringSerializer, byteArraySerializer);

			// Query column family with name
			indexedSlicesQuery.setColumnFamily(_columnFamilyName);
			// Set the columns you want back
			indexedSlicesQuery.setColumnNames(FIRSTNAME_COLUMN_NAME, MIDDLENAME_COLUMN_NAME,
					LASTNAME_COLUMN_NAME);
			// A mandatory equals method. Find the firstname10 in column with name firstname
			indexedSlicesQuery.addEqualsExpression(FIRSTNAME_COLUMN_NAME,
					stringSerializer.toBytes("firstname10"));
			// Do the query
			QueryResult<OrderedRows<String, String, byte[]>> result = indexedSlicesQuery
					.execute();

			boolean found = false;
			// result is ....
			Iterator<Row<String, String, byte[]>> queryResultIterator = result.get().iterator();

			while (queryResultIterator.hasNext()){
				// fetch the result and translate the content of the column
				Row<String, String, byte[]> queryRow= queryResultIterator.next();
				String firstName = stringSerializer.fromBytes(queryRow.getColumnSlice().getColumnByName("firstName").getValue());
				Assert.assertEquals("Search string should match query string", firstName , "firstname10");
				found  = true;
			}
			if (!found){
				fail("Query should match at least 1 record");
			}

		} catch (Throwable t) {
			fail(t.toString());
		}

	}
	
	// Test with lte comparator.
	@Test
	public void sliceLongLteTest() {

		try {

			IndexedSlicesQuery<String, String, byte[]> indexedSlicesQuery = HFactory
					.createIndexedSlicesQuery(_keyspace, stringSerializer,
							stringSerializer, byteArraySerializer);
			
			indexedSlicesQuery.setColumnFamily(_columnFamilyName);
			indexedSlicesQuery.setColumnNames(FIRSTNAME_COLUMN_NAME, MIDDLENAME_COLUMN_NAME,LASTNAME_COLUMN_NAME);
			indexedSlicesQuery.addEqualsExpression(FIRSTNAME_COLUMN_NAME,stringSerializer.toBytes("firstname10"));
			indexedSlicesQuery.addLteExpression(CREATIONTIME_COLUMN_NAME,longSerializer.toBytes(System.currentTimeMillis()));
			QueryResult<OrderedRows<String, String, byte[]>> result = indexedSlicesQuery.execute();
			String firstname = stringSerializer.fromBytes(result.get().iterator()
					.next().getColumnSlice().getColumnByName(FIRSTNAME_COLUMN_NAME)
					.getValue());
			Assert.assertEquals(firstname, "firstname10");
		} catch (Throwable t) {
			t.printStackTrace();
		}

	}	
	
	
}
