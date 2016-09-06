package pythiaHbase;

import java.io.IOException;

public class PythiaFactory 
{
	public static PythiaInterface getPythia(double f, double s, double t, int k, String tableName, int cellWidth) throws IOException
	{
		//return new PythiaBasedHbaseDistance(k, f,s,t);
		
		return new PythiaQuadAndHbaseDistance(k, tableName, cellWidth);
	}

}
