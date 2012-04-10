package nl.kaninefaten.cassandra.tutorial.t5;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Test;

/**
 * Generates a dataset for demonstrating querying samples.
 * <p>
 * Creates a dataset which is used in the query samples.
 * 
 * @author Patrick van Amstel
 * @date 2012 04 05
 */
public class CassandraDatasetCreator {
	
	private static int _numberOfRecords 		= 100;
	private static File _baseFolder = new File("src/test");
	private static String _datasetFileName 	= "cassandra-testset.txt";
	
	private static String _encoding 			= "UTF-8";
	
	private static ExampleModelObject createExampleModelObject(int counter){
		ExampleModelObject exampleModelObject = new ExampleModelObject();
		exampleModelObject.setKey("key." + counter);
		exampleModelObject.setFirstName("firstname" + counter);
		exampleModelObject.setMiddleName("middleName" + counter);
		exampleModelObject.setLastName("lastName" + counter);
		exampleModelObject.setEmailAdress("email"+counter+"@iets.nl");
		exampleModelObject.setLoginName("loginnaam" + counter);
		exampleModelObject.setPassword("RandomizedPassWord " + counter);
		exampleModelObject.setCreationTime(System.currentTimeMillis());
		exampleModelObject.setUpdateTime(System.currentTimeMillis());
		return exampleModelObject;
	}
	
	public static File datasetFileHandler(){
		return new File(_baseFolder , _datasetFileName);
	}
	
	@Test
	public void createDataSet() {
		FileOutputStream fout = null;
		_baseFolder.mkdirs();
		if (!_baseFolder.exists()){
			fail("Could not create baseFolder " + _baseFolder.getAbsolutePath());
			return;
		}
		try {
			fout = new FileOutputStream(datasetFileHandler());
			for (int counter = 0; counter < _numberOfRecords; counter++) {
				ExampleModelObject exampleModelObject = createExampleModelObject(counter);
				fout.write(exampleModelObject.toTabString().getBytes(_encoding));
				fout.write("\n".getBytes(_encoding));
			}
		} catch (IOException e) {
			fail(e.toString());
		} finally {
			if (fout != null) {
				try {
					fout.flush();
					fout.close();
				} catch (IOException e) {
					fail(e.toString());
				}
			}
		}
	}	
	
}
