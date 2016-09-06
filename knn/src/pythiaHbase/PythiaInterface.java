package pythiaHbase;

import java.io.IOException;
import java.net.URISyntaxException;

public interface PythiaInterface 
{
	public void constructGloablSignature(String summary) throws IOException;
	
	public String getCandidateRows() throws IOException, URISyntaxException;
	
	public String getCandidateRows(double radius,double [] center) throws IOException, URISyntaxException;
	
	public long idRowsTime();
			
	public void setMyQry(double[] myQry);
	
	public void setK(int k);
	
	public double getMaxDis();
	
	public void close() throws IOException;
	 
	

}
