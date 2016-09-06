package queryCell;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import pythiaHbase.AscendingHeapSort;
import pythiaHbase.DataDistance;
import pythiaHbase.Helper;
import quadTree.Box;

public class Rows {
  private double mKd=-1,width=-1;
  private double [] center={0,0};
  private double radius=0;
  private long timeGetQuery;
  private long knnTime=0;
  private long timeIdRows;

  public Rows(double maxDist, double[] ds, double[] cntr, double qryWidth)
  {
    this.mKd=maxDist;
    this.center=cntr;
    this.width=qryWidth;


  }

  public double[] getCenter()
  {
    return center;
  }

  public double getRadius()
  {
    this.radius = Math.sqrt(2)*(width/2)+mKd;
    return radius;
  }

  public double ecuDis(double[] qry, double[] data)
  {
    
    double dis = 0;
    long startTime=System.currentTimeMillis();
    for (int i = 0; i < qry.length; i++) //goes through query points 
    { 
      dis = dis +Math.pow((qry[i] - data[i]), 2);
    }
    long endTime=System.currentTimeMillis();
    knnTime=knnTime+(endTime-startTime);
    return Math.sqrt(dis);

  }


  public ArrayList<double[]> getDataPoints(String keySet,String tablename) throws IOException
  {
    Configuration conf =   HBaseConfiguration.create(); 

    Connection conn  = ConnectionFactory.createConnection(conf);

    Table table = conn.getTable(TableName.valueOf(tablename));

    ArrayList<double[]> dataPoints=new ArrayList<double[]>();

    timeGetQuery=0;

    long startTime = System.currentTimeMillis();

    for (String key : keySet.split(","))
    {      

      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get); // gets the query row from the dataset hbase table 



      byte [] value = result.getValue(Bytes.toBytes("C"), Bytes.toBytes("x"));// gets pairs of coords 


      for (String x : Bytes.toString(value).split("\t")) //  takes one pair of coords 
      {
        dataPoints.add(Helper.parseStringToVector(x));
      }

    }
    conn.close();
    conf.clear();
    table.close();
    long endTime = System.currentTimeMillis();

    timeGetQuery=timeGetQuery+(endTime-startTime);
    return dataPoints;
  }

  public String getCandidateRows(double radius, double[] center, int cellWidth) throws IOException, URISyntaxException // need this !!
  { 
    String path = "";
    
    Box box = new Box(center[0],center[1],radius);

    ArrayList<double[]>canRows=new ArrayList<double[]>();
    long startTime=System.nanoTime();
    double [] min=findRow(box.getMinX(),box.getMinY(),cellWidth);
    double [] max=findRow(box.getMaxX(), box.getMaxY(),cellWidth);

    double sumX=min[0];

    while(sumX<=max[0])
    {
      double sumY=min[1];
      while(sumY<=max[1])
      {

        canRows.add(new double[] {sumX,sumY});
        sumY=sumY+cellWidth;
      }
      sumX=sumX+cellWidth;
    }
    long endTime=System.nanoTime();
    timeIdRows=endTime-startTime;
    for(double [] p:canRows)
    {
      path=path+(int)p[0]+"-"+(int)p[1]+",";
    }
    path=path.substring(0, path.length()-1);
    return path;
  }

  public double[] findRow(double x, double y,int width)
  {
    x=Math.max(0.0,x);
    x=Math.min(Math.pow(10, 6)-0.001, x);
    y=Math.max(0.0,y);
    y=Math.min(Math.pow(10, 6)-0.001, y);
    int i =(int) (x/width);
    int j =(int) (y/width);
    double [] row=new double[2];
    row[0]=i*width;
    row[1]=j*width;
    return row;
  }

  public long rowIdTime()
  {
    return timeIdRows;
  }
  public long qryTime()
  {
    return timeGetQuery;
  }

  public long knnTime()
  {
    return knnTime;
  }



}
