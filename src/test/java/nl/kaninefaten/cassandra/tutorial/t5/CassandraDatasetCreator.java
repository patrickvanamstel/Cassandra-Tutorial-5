package nl.kaninefaten.cassandra.tutorial.t5;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Test;

/**
 * Generates a dataset for demonstrating querying samples.
 * 
 * @author Patrick van Amstel
 * @date 2012 04 05
 */
public class CassandraDatasetCreator {
	
	private static int numberOfRecords 		= 100;
	private static String datasetFileName 	= "cassandra-testset.txt";
	private static String encoding 			= "UTF-8";
	
	private static ExampleModelObject createExampleModelObject(int counter){
		ExampleModelObject exampleModelObject = new ExampleModelObject();
		exampleModelObject.setKey("key." + counter);
		exampleModelObject.setFirstName("firstname" + counter);
		exampleModelObject.setMiddleName("middleName" + counter);
		exampleModelObject.setLastName("lastName" + counter);
		exampleModelObject.setEmailAdress("email"+counter+"@iets.nl");
		exampleModelObject.setPassword("RandomizedPassWord " + counter);
		exampleModelObject.setCreationTime(System.currentTimeMillis());
		exampleModelObject.setUpdateTime(System.currentTimeMillis());
		return exampleModelObject;
	}

	
	
	@Test
	public void createDataSet() {

		FileOutputStream fout = null;
		File baseFolder = new File("src/test");
		baseFolder.mkdirs();
		if (!baseFolder.exists()){
			fail("Could not create baseFolder " + baseFolder.getAbsolutePath());
			return;
		}
		
		File dataSetFile = new File(baseFolder , datasetFileName);
		try {

			fout = new FileOutputStream(dataSetFile);
			for (int counter = 0; counter < numberOfRecords; counter++) {
				ExampleModelObject exampleModelObject = createExampleModelObject(counter);
				fout.write(exampleModelObject.toTabString().getBytes(encoding));
				fout.write("\n".getBytes(encoding));
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
