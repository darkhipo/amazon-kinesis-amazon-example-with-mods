package darkhipo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.amazonaws.services.kinesis.AmazonKinesis;

public class Utils {

	public static AmazonKinesis kinesisClient;
	static final String pathToLastSeq = "lastSeq.bak";
	
	public static String getLastSeqNum( ){
		File f = new File(pathToLastSeq);
		if( f.exists() && !f.isDirectory() ) { 
			try {
				FileReader fr = new FileReader( f.getAbsoluteFile() );
				BufferedReader br = new BufferedReader(fr);
				String key = br.readLine();
				br.close();
				return key;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	public static void writeLastSeqNum(String last){
		File f = new File(pathToLastSeq);
		try {
			FileWriter fw = new FileWriter( f.getAbsoluteFile(), false );
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write( String.format("%s%n", last) );
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
