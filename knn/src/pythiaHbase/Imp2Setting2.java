package pythiaHbase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import queryCell.*;

public class Imp2Setting2 {

  public static  int k = 100 ;

  public static String summary = "/home/shadoop/Data/20kkIndexed/"; // Index folder for big Dataset 

  public static String tableName = "20kkW10k";

  public static int cellWidth = 10000; // Large Dataset
  public static double queryWidth = 100000; // Query Dataset
  public static String queryTable = "200kW100k"; //Query Table
  public static PythiaInterface py;
  public static GetDataHbase table;
  public static queryCell.Limits Cell;

  public static void main(String[] args) throws IOException, URISyntaxException 
  {
    py =  PythiaFactory.getPythia(10000,150,30,k,tableName, cellWidth);
    table = new GetDataHbase(k,tableName);
    constructQuadTree();
    py.setK(k);
    table.setK(k);

    BufferedWriter resu = new BufferedWriter(new FileWriter("/home/shadoop/Data/Results20kkW10k-200kW100k.csv",true)) ; //Results file 
    BufferedReader br = new BufferedReader(new FileReader("/home/shadoop/Data/200kIndexedW100k/heads-r-00000")); // Query index file 
    String readIndex="";
    String results ="Identify_rows_time,"+"Get_candidate_rows,"+"Get_query_row_from_hbase,"+"NumberOfPoints,"+"Do_KNN"+"\n"; //results file header
    resu.write(results);
    resu.flush(); 
       
    while((readIndex=br.readLine())!=null)
    {
      results="";

      String str = readIndex.split("\t")[1];
      
      Cell = new Limits(str,queryWidth);
      ArrayList<double[]> CornerList=new ArrayList<double[]>();

      CornerList=Cell.returnCorners();
      double mKd=-1;

      for(int i=0;i<4;i++) // iterating through the 4 corners 
      { 
        double[] cl = CornerList.get(i);
        py.setMyQry(cl);
        String cRows = py.getCandidateRows();
        double maxD = table.getKNNThMaxDistance(cRows,cl);
        if(maxD>mKd) // establishing the maximum distance to the Kth point between corners 
        {
          mKd=maxD;

        }
      }


      Rows rows =new Rows(mKd,Cell.getRow(),Cell.getCenter(),Cell.getWidth());
      Cell=null; //emptying some memory
      String cRows=rows.getCandidateRows(rows.getRadius(), rows.getCenter(),cellWidth);
      results=results+rows.rowIdTime()+","; // Adding to results file
      ArrayList<double[]>canRows =rows.getDataPoints(cRows,tableName);
      results=results+rows.qryTime()+","; // Adding to results file
      ArrayList<double[]>canQry=rows.getDataPoints(str,queryTable);
      results=results+rows.qryTime()+","; // Adding to results file
      results=results+canRows.size()+","; // Adding to results file

      while(!canQry.isEmpty()) //calculating ecu distance by emptying the query list of values one at a time 
      {
        double []y=canQry.remove(0);
        for (double [] x: canRows)  
        {
          rows.ecuDis(y, x);
        }
      }

      results=results+rows.knnTime()+"\n"; // Adding to results file
      resu.write(results); 
      canRows.clear();
      canQry.clear();

    }

    br.close();
    resu.flush();
    resu.close(); 


    table.closeTable();
    py.close();


    System.err.println("Job Completed");

    System.exit(0);


  }

  public static void constructQuadTree() throws IOException
  {
    System.out.println("constructing signature.........");

    py.constructGloablSignature(summary);

    System.out.println("signature loaded..........");

  }



}
