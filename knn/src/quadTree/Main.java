package quadTree;

 
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.xerial.snappy.Snappy;
public class Main {


	public static void main(String[] args) throws UnsupportedEncodingException, IOException  
	{
		String input = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
			     + "Snappy, a fast compresser/decompresser.";
		byte[] compressed = Snappy.compress(input.getBytes("UTF-8"));
		byte[] uncompressed = Snappy.uncompress(compressed);

		String result = new String(uncompressed, "UTF-8");
		System.out.println(result);

	} 
}
	
	



 
