package pythiaHbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import quadTree.Box;
import quadTree.Point;
import quadTree.QuadTree;

public class PythiaQuadAndHbaseDistance implements PythiaInterface
{
  QuadTree qTree;

  private Point myQry;

  private int k;

  private double maxDistance;
  
  private long candidateTime=0;

  HbaseDistance conn;

  public PythiaQuadAndHbaseDistance(int k, String tableName, int cellWidth) throws IOException 
  {
    qTree = new QuadTree(500000.0,500000.0,500000.0,100,cellWidth); //500k
    this.k = k;
    conn = new HbaseDistance(tableName);

    this.maxDistance = Double.MAX_VALUE;

  }


  public void constructGloablSignature(String summary) throws IOException {

    int x =0; 	int y=1;
    File[] files = new File(summary).listFiles();
    
    for (int i = 0; i < files.length; i++) 
    {
      BufferedReader br = new BufferedReader(new FileReader(summary+files[i].getName()));
      String line = "";
      while (( line = br.readLine() )!= null)
      {

        String[] mline = line.split("\t");

        String[] coor = mline[1].split("-");

        qTree.insert(new Point(Double.valueOf(coor[x]),Double.valueOf(coor[y]),Integer.valueOf(mline[0]), mline[1] ));


      }
      br.close();
      //System.out.println(count);
    }
    qTree.leafNode();


  }

  @Override
  public String getCandidateRows() throws IOException, URISyntaxException // need this !!
  { 
    int totalNumberOFK = 0;  boolean enoughK = false;  String path = "";

    Box box = new Box();

    AscendingHeapSort candidates = new AscendingHeapSort(); 
    long startTime = System.currentTimeMillis();
    
    qTree.getCandidateRegion(myQry,box);

    qTree.getCanIndexRange(box, candidates, myQry);

    DataDistance cand =null;	

    while (!candidates.isEmpty())
    {
      cand = candidates.removeMin();

      totalNumberOFK += cand.getNumDataItems();

      if ((!enoughK) && (totalNumberOFK >= (long)k))
      {
        enoughK = true;

        this.maxDistance = getExacMaxtDis(cand.getRowKey());

      }
      if ( cand.getMinDis() <= this.maxDistance ) 
      { 
        if (path == "")
        {
          path = cand.getRowKey();

        }
        else
        {
          path+=","+cand.getRowKey();
        }
      }
      else
      {
        break;
      }
    }

    long endTime=System.currentTimeMillis();
    candidateTime=endTime-startTime;
    return path;
  }
  
  public long idRowsTime()
  {
    return candidateTime;
  }


  public String getCandidateRows(double radius, double[] center) throws IOException, URISyntaxException // need this !!
  { 
    String path = "";

    Box box = new Box(center[0],center[1],radius);

    AscendingHeapSort candidates = new AscendingHeapSort();

    qTree.getCandidateRegion(myQry,box);

    qTree.getCanIndexRange(box, candidates, myQry);

    DataDistance cand =null;  

    while (!candidates.isEmpty())
    {
      cand = candidates.removeMin();

      if (path == "")
      {
        path = cand.getRowKey();

      }
      else
      {
        path+=","+cand.getRowKey();
      }


    }

    return path;
  }


  public double getExacMaxtDis(String rowID) throws IOException
  { 
    return conn.getMaxDis(rowID,new double [] {myQry.getX(), myQry.getY()});

  }

  @Override
  public void setMyQry(double[] myQry)
  {
    myQry[0]=Math.min(myQry[0], Math.pow(10, 6)-0.0001);
    myQry[1]=Math.min(myQry[1], Math.pow(10, 6)-0.0001);
    this.myQry = new Point(myQry[0], myQry[1]);		
  }

  @Override
  public void setK(int k) 
  {
    this.k = k;	
    conn.setK(k);
  }

  @Override
  public double getMaxDis() 
  {
    return this.maxDistance;
  }

  @Override
  public void close() throws IOException 
  {
    conn.close()	;	
  }


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  private class HbaseDistance
  {
    public GetDataHbase table;

    public HbaseDistance(String tableName) throws IOException
    {

      table = new GetDataHbase(k,tableName);
    }

    public double getMaxDis(String keySet, double[] strQry) throws IOException
    {
      return table.getKNNThMaxDistance(keySet, strQry);
    }

    public void setK(int k)
    {
      table.setK(k);
    }

    public void close() throws IOException
    {
      table.closeTable();
    }

  }



}
