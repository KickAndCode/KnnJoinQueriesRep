package generate;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/** 
 Generate pseudo-random floating point values, with an 
 approximately Gaussian (normal) distribution.

 Many physical measurements have an approximately Gaussian 
 distribution; this provides a way of simulating such values. 
 */
public final class RandomGaussian
{
	//private static final Log LOG = LogFactory.getLog("op");
	private static Random fRandom = new Random();
	static ArrayList<String[]> host = new ArrayList<String[]>();
	static int numMultiVariate = 101;
	static int numDataPoints = 6000000;
	static double meanInitial= 9500.0f;
	static double varianceInitial = 500.0f;
	public static void main(String[] args) throws IOException
	{
		int count = 0;
		
		for (int x = 1; x < numMultiVariate ; x++)
		{
			
		RandomGaussian gaussian = new RandomGaussian();
		
		for (int i =0; i < numDataPoints; i++)
		{
			double aMEAN = meanInitial * x; 
			double aVARIANCE = varianceInitial;
			String[] vec = new String[2];
			for (int idx = 0; idx < 2; idx++)
			{
				double myvalue = getGaussian(aMEAN, aVARIANCE);
				//if (myvalue > 10000) count++;
				
				if (Double.compare(myvalue, 1000000.00)> 0) myvalue = 1000000.00;
				
				 
				vec[idx] = String.valueOf(myvalue);
				 
					
			}
			
			host.add(vec);
			if (host.size() <= 1000) write();
		}
		   if (!host.isEmpty()) write();
		}
		System.out.println(count);
	}

	

	private static double getGaussian(double aMean, double aVariance)
	{
		double gen = fRandom.nextGaussian();
		while (gen > 1.00 || gen < -1.00)
		{
			gen = fRandom.nextGaussian();
		}
		 
		//System.out.println(gen);
		return aMean + gen * aVariance;
	}

	 private static void write() throws IOException
	 {
		 BufferedWriter bw = new BufferedWriter(new FileWriter("/home/atoshum_user/MultivariateDataSet_Two.csv", true));
			while(!host.isEmpty())
			{
				String[] x =host.remove(host.size()-1);
				bw.write(x[0]+","+x[1]+"\n");
			}
			bw.close();
	 }
} 