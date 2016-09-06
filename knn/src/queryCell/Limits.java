package queryCell;

import java.util.ArrayList;

public class Limits {
  private String currRow="";
  private double width=100000; //this will need to be edited 
  private double [] SW={0,0},NW={0,0},SE={0,0},NE={0,0};
  private double [] center={0,0};
  private ArrayList<double[]> CornerList=new ArrayList<double[]>(4);
  public Limits(String cRow, double width)
  {
    this.currRow=cRow;
    this.width=width;
    cellCorners();
  }

  public void setWidth(double width)
  {
    this.width=width;
  }

  public double[] getRow()   //get row in an int array , instead of string 
  {
    double [] row =new double [2];
    if(currRow.isEmpty())     
    {
      System.out.println("error");
      return null;
    }
    else
    {
      String [] rowCoord = new String[2];
      rowCoord=currRow.split("-"); // rows are 0-0 0-25000 .. split at - and save in int array 
      for(int i=0;i<2;i++)
      {
        row[i]=Double.parseDouble(rowCoord[i]);

      }
      return row;
    }

  }

  public void cellCorners()    // calculate the 4 corners of the cell using the row-key and width 
  {
    double [] row=new double[2];
    row=getRow();
    this.SW=row;

    this.NW[0]=row[0];
    this.NW[1]=row[1]+width;

    this.SE[0]=row[0]+width;
    this.SE[1]=row[1];

    this.NE[0]=row[0]+width;
    this.NE[1]=row[1]+width;

    this.center[0]=row[0]+width/2;
    this.center[1]=row[1]+width/2;

    setCorners(); 
  }

  public void setCorners()
  {
    this.CornerList.add(SW);
    this.CornerList.add(NW);
    this.CornerList.add(SE);
    this.CornerList.add(NE);
  }

  public ArrayList<double[]> returnCorners()
  {
    return CornerList;

  }

  public double [] getCenter()
  {
    return center;
  }

  public double getWidth()
  { 
    return width;    
  }

}
