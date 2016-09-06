package baseline;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import pythiaHbase.*;
public class NestedLoopRun {

  public static void main(String[] args) throws IOException {

    BufferedReader q = new BufferedReader(new FileReader("/home/shadoop/Data/qry200k"));
    BufferedReader d = new BufferedReader(new FileReader("/home/shadoop/Data/20kkDataset"));
    BufferedWriter resu=new BufferedWriter(new FileWriter("/home/shadoop/Data/benchlineRunTimes.csv"));

    String results="KnnTime"+"\n";
    
    resu.write(results);
    resu.flush();
    String readQ="";
    String readD="";
    int count=0;
    long startTime=System.currentTimeMillis();
    while((readQ=q.readLine())!=null)
    {
      System.out.println(readQ);
      while((readD=d.readLine())!=null)
      {
        System.out.println(readD);
        ecuDis(Helper.parseStringToVector(readQ),Helper.parseStringToVector(readD));
      }
    }
    long endTime=System.currentTimeMillis();
    resu.write(endTime-startTime+"");
    resu.flush();
    q.close();
    d.close();
    resu.close();
  }
  
  public static void ecuDis(double[] qry, double[] data)
  {
    double dis = 0;

    for (int i = 0; i < qry.length; i++) //goes through query points 
    { 
      dis = dis +Math.pow((qry[i] - data[i]), 2);
    }
    Math.sqrt(dis);
  }
}
