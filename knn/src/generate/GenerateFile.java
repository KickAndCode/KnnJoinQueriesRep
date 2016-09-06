package generate;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class GenerateFile {


  public static void main(String[] args) throws IOException
  {

    BufferedWriter br = new BufferedWriter(new FileWriter (new File("/home/shadoop/Data/Dataset1Mil"),true ));

    Random rd = new Random();

    double min = 1.0;

    double max = 1000000.0;

    int dimension = 2;

    int numberOfPointsInDataSet =  (int)Math.pow(10, 6);

    StringBuilder str = new StringBuilder("");

    int writeInterval = 10 * (int)Math.pow(10, 2);

    int count = 0;
    long time = System.currentTimeMillis();
    while (count < numberOfPointsInDataSet)
    {

      for (int i =0 ; i < dimension; i++)
      {
        double rnd = min + (max - min)*rd.nextDouble();

        if (Double.compare(1.0, rnd) >= 0 || Double.compare(1000000.0, rnd) < 0) System.out.println("=================================");
        if (i == 0)
        {
          str.append(String.valueOf(rnd));
        }
        else
        {
          str.append( ","+String.valueOf(rnd)+"\n");
          System.out.println(count);
        }


      }

      count++;

      if ((count % writeInterval ) == 0)
      {
        br.write(str.toString());
        str = new StringBuilder("");
      }
    }
    if ((count % writeInterval ) != 0 && str.toString()!="")  
    {
      br.write(str.toString());
      str = new StringBuilder("");
      count = 0;

    }	 
    System.out.println("Closing Writer. Data Generated");
    br.close();


  }



}


