package pythiaHbase;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import queryCell.*;

public class FirstImplementation {

  public static  int k = 100 ;

  public static final String hostAdd = "hdfs://localhost:9000";//"hdfs://localhost:9000";

  public static String summary = "/home/shadoop/Data/20kkIndexed/";

  public static String tableName = "20kkW10k";

  public static double maxDistance = 0;

  public static int cellWidth = 10000; // 10k // 

  public static double[] myQry;
  public static GetDataHbase table ;
  public static void main(String[] args) throws Exception 
  {
    PythiaInterface py = PythiaFactory.getPythia(10000,150,30,k,tableName, cellWidth);
    table = new GetDataHbase(k,tableName);
    System.out.println("constructing signature.........");

    py.constructGloablSignature(summary);

    System.out.println("signature loaded..........");


   
    // -------------------------------------------------------------------//  
         
		 BufferedReader br = new BufferedReader(new FileReader("/home/shadoop/Data/qry200k"));
		 BufferedWriter resu = new BufferedWriter(new FileWriter("/home/shadoop/Data/Results-Implementation1.csv",true)) ;
			py.setK(k);

			table.setK(k) ;

			String str = null;

			int count =0;

			while ((str = br.readLine())!= null)
			{				
			  count++;
			  System.out.println(count);

				myQry = Helper.parseStringToVector(str); // this parses the raw qry file to an array of coord values x,y,x,y,x,y ... 

				py.setMyQry(myQry); // sets query as the parsed query array 

				long startTime = System.currentTimeMillis();

				String candidateCelles = py.getCandidateRows();  // identify relevant rows

				long endPythia = System.currentTimeMillis();

				long dataRetrievalTime = table.cmptKNNMultiGet(candidateCelles, myQry); // 


				long endTime = System.currentTimeMillis();

				resu.write(String.valueOf(endPythia - startTime )+","+String.valueOf(dataRetrievalTime)+","+String.valueOf((endTime-endPythia)-dataRetrievalTime) + ","+ String.valueOf(endTime - startTime)+","+k+","+candidateCelles.split(",").length+","+py.getMaxDis()+","+table.getQualtitativeMeasure()+ "\n");

				 table.printKNN();
			}

			

		br.close();
    table.closeTable();
    System.err.println("Job Completed");
    
    py.close();
   
    resu.close();

    System.exit(0);

  }


}
