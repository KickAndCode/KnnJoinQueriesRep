package pythiaHbase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class GetDataHbase {

  String tablename;

  PriorityQueue<DataDistance> prtyQueue;

  double[] qry;  	

  int k ; 	

  Configuration conf;

  Connection conn; 

  Table table;

  int qualtitativeMeasure; 

  long timeGetQuery=0 ,  timeGetRows=0, timeKNN;

  public GetDataHbase(int k, String tableName) throws IOException
  {
    this.k = k;

    this.tablename = tableName;

    qualtitativeMeasure = 0;

    conf =   HBaseConfiguration.create(); 

    conn  = ConnectionFactory.createConnection(conf);

    table = conn.getTable(TableName.valueOf(tablename));

  }
  public ArrayList<double[]> getDataPoints(String keySet) throws IOException
  {
    ArrayList<double[]> dataPoints=new ArrayList<double[]>();

    Get queryRowList ;

    long startTime = System.currentTimeMillis();

    for (String key : keySet.split(","))
    {      
      queryRowList=new Get(Bytes.toBytes(key));

      Result result = table.get(queryRowList); // gets the query row from the dataset hbase table 

      byte [] value = result.getValue(Bytes.toBytes("C"), Bytes.toBytes("x"));// gets pairs of coords 


      for (String x : Bytes.toString(value).split("\t")) //  takes one pair of coords 
      {
        dataPoints.add(Helper.parseStringToVector(x));
      }

    }
    long endTime = System.currentTimeMillis();
    timeGetQuery=endTime-startTime;
    return dataPoints;
  }

  public long cmptKNNMultiGet(String keySet, double[] strQry ) throws IOException
  {
    prtyQueue = initilizePriorityKey();

    List<Get> queryRowList = new ArrayList<Get>();

    qualtitativeMeasure = 0;

    this.qry = strQry;

    long startTime = System.currentTimeMillis();

    for (String key : keySet.split(","))
    {			 
      queryRowList.add(new Get(Bytes.toBytes(key)));
    }
    Result[] result = table.get(queryRowList); // gets the query row from the dataset hbase table 
    

    long endTime = System.currentTimeMillis();

    for (Result rst : result) //goes through the values of the row one by one 
    {
      byte [] value = rst.getValue(Bytes.toBytes("C"), Bytes.toBytes("x"));// gets pairs of coords 

      for (String x : Bytes.toString(value).split("\t")) //  takes one pair of coords 
      {
        qualtitativeMeasure += sortData(x); // sortData takes as input ("x,y") 
      }
    }

    return endTime - startTime;
  }

  public long cmptKNNSingleGet(String keySet, double[] strQry) throws IOException
  {

    prtyQueue = initilizePriorityKey();

    qualtitativeMeasure = 0;

    this.qry =  strQry;

    long time = 0;

    for (String key : keySet.split(","))
    {

      long startTime = System.currentTimeMillis();

      Get get = new Get(Bytes.toBytes(key));

      Result result = table.get(get);

      time += (System.currentTimeMillis() - startTime);

      byte [] value = result.getValue(Bytes.toBytes("C"), Bytes.toBytes("x"));



      for (String x : Bytes.toString(value).split("\t")) // Sort data calculates eucDistance between points 
      {                                                   
        qualtitativeMeasure += sortData(x);
      }
    }
    return time;
  }


  public double[] toDoubleVec(String line)
  {
    String[] strVec = line.split(",");
    int length = strVec.length;

    double[] dblVec = new double[length];


    for (int i = 0; i < length ; i++)
    {
      dblVec[i] = Double.valueOf(strVec[i]);
    }

    return dblVec;

  }

  public double ecuDis(double[] qry, double[] data)
  {

    double dis = Double.MAX_VALUE;
    timeKNN=0;
    long startTime=System.currentTimeMillis();
    for (int i = 0; i < qry.length; i++) //goes through query points 
    { 

      if (i == 0)
      {
        dis = Math.pow((qry[i] - data[i]), 2);

      }
      else       //calculates distance between query points and a point from the data set 

      {
        dis += Math.pow((qry[i] - data[i]), 2); 

      }



    }
    long endTime=System.currentTimeMillis();
    timeKNN=endTime-startTime;
    return Math.sqrt(dis);

  }

  public void setTableName(String tablename)
  {
    try {
      table = conn.getTable(TableName.valueOf(tablename));
    } catch (IOException e) {

      e.printStackTrace();
    }
  }

  public void printKNN()
  {
    int count = this.k;
    while(count > 0 )
    {
      DataDistance x = prtyQueue.poll();
      System.out.println(x.getDistance()+"\t"+x.vecDistanceString());
      count--;
    }
  }

  public int getQualtitativeMeasure()
  {
    return this.qualtitativeMeasure;
  }

  public int getK() {
    return k;
  }

  public void setK(int k) {
    this.k = k;
  }

  public void closeTable() throws IOException
  {
    this.table.close();
    this.conn.close();
    this.conf.clear();
  }

  public int sortData(String x) // calculates distance between query points and (x,y) from DataSet 
  {	  
    double[] vec = toDoubleVec(x);

    double dis = ecuDis(qry, vec);

    if (dis == Double.MAX_VALUE)

      return 0;

    prtyQueue.add(new DataDistance(vec,dis));


    return 1;

  }

  public PriorityQueue<DataDistance> initilizePriorityKey()
  {
    return new PriorityQueue<DataDistance>(k , new Comparator<DataDistance>()
    {
      @Override
      public int compare(DataDistance x, DataDistance y)
      {
        return  Double.compare(x.getDistance(), y.getDistance());
      }
    });
  }


  public double getKNNThMaxDistance(String keySet, double[] strQry) throws IOException
  {

    //cmptKNNSingleGet(keySet, strQry );
    cmptKNNMultiGet(keySet, strQry);

    int count = 1;

    while (!prtyQueue.isEmpty())
    {
      if (count == k)
      {
        DataDistance x= prtyQueue.poll();

        return x.getDistance();
      }
      prtyQueue.poll();
      count++;
    }

    return Double.MAX_VALUE;
  }


  public long knnTime()
  {
    return timeKNN;
  }

  public long qryTime()
  {
    return timeGetQuery;
  }


}
