package nl.kaninefaten.cassandra.tutorial.t5;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;


/**
 * Loads a sets of ExampleModelObject in memory.
 * <p>
 * Note:
 * This is a test class.
 * 
 *  
 * @author Patrick van Amstel
 * @date 2012 04 05
 *
 */
public class CassandraDatasetLoader {
	
	private static ArrayList<ExampleModelObject> _exampleModelObjectList = new ArrayList<ExampleModelObject>();

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
	
	public void iterator(InputStream inputStream) throws IOException {
		InputStreamReader reader = new InputStreamReader(inputStream, "UTF-8");
		char[] c = new char[1];
		StringBuilder builder = new StringBuilder();
		while (reader.read(c) > -1) {
			if (c[0] == '\n') {
				processLine(builder.toString());
				builder = new StringBuilder();
				continue;
			}
			builder.append(c[0]);
		}
		processLine(builder.toString());
	}

	private void processLine(String lineRecord) {
		if (lineRecord == null || lineRecord.equals("")) {
			return;
		}

		ExampleModelObject object = ExampleModelObject
				.fromTabString(lineRecord);
		_exampleModelObjectList.add(object);

	}

	public static ArrayList<ExampleModelObject> getExampleModelObjectList() {
		return _exampleModelObjectList;
	}
	
}
