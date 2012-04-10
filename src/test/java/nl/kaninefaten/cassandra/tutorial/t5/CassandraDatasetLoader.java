package nl.kaninefaten.cassandra.tutorial.t5;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;


/**
 * 
 * @author Patrick van Amstel
 * @date 2012 04 05
 *
 */
public class CassandraDatasetLoader {

	
	public static File dataFile = new File("/media/WerkDrive/Projects/Anachron/research/cassandra/Cassandra-Tutorial-5/src/test/cassandra-testset.txt");
	
	public static ArrayList<ExampleModelObject> exampleModelObjectList = new ArrayList<ExampleModelObject>();
	
	public void iterator(File file) throws IOException{
		FileInputStream input = null;
		try
		{
			input = new FileInputStream(file);
			iterator(input);
		}finally{
			if (input != null){
				input.close();
			}
		}
	}
	
	public void iterator(InputStream inputStream) throws IOException{
		InputStreamReader reader = new InputStreamReader(inputStream, "UTF-8");
		char [] c = new char[1];
		StringBuilder builder = new StringBuilder();
		while(reader.read(c) > -1){
			//System.out.println(c[0]);
			if (c[0] == '\n'){
				processLine(builder.toString());
				builder = new StringBuilder();
				continue;
			}
			builder.append(c[0]);
		}
		processLine(builder.toString());
	}
	
	
	private void processLine(String lineRecord) {
		if (lineRecord == null || lineRecord.equals("")){
			return;
		}
		
		ExampleModelObject object = ExampleModelObject.fromTabString(lineRecord); 
		exampleModelObjectList.add(object);
	
	}



	public static void main (String [] args){
		CassandraDatasetLoader loader = new CassandraDatasetLoader();
		try {
			loader.iterator(dataFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
