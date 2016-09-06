package pythiaHbase;

import java.util.ArrayList;
public class CellSummary {

  private String rowKey ;

  private int numberOfDataItems;

  private double[] vecFormatAddress;

  private ArrayList<Integer> indices;

  private int indexID;


  public CellSummary(String address, int numOFDItems)
  {
    this.rowKey = address;

    this.numberOfDataItems = numOFDItems;

    vecFormatAddress = parseStringToVector(address);

    indices = new ArrayList<Integer>();

    indices.add(address.hashCode());

  }

  public CellSummary(String address,int numberOfDataItems,  int childID )
  {
    this.rowKey = address;

    vecFormatAddress = parseStringToVector(address);

    this.numberOfDataItems = numberOfDataItems;

    indices = new ArrayList<Integer>();

    indices.add(childID);




  }

  public ArrayList<Integer> getIndices()
  {
    return indices;
  }

  public void addDataToACell(int indexID, int numItmes )
  {
    indices.add(indexID);

    this.numberOfDataItems +=  numItmes;
  }

  public String getRowKey() 
  {
    return rowKey;

  }

  public int getNumberOfDataItems() 
  {
    return numberOfDataItems;
  }

  public void setNumberOfDataItems(int numberOfDataItems) 
  {
    this.numberOfDataItems = numberOfDataItems;
  }
  public void increamentNumberOFDataItems(int numberofDataItmes)
  {
    this.numberOfDataItems+=numberofDataItmes;
  }

  public double[] getVec()
  {
    return this.vecFormatAddress;
  }

  public double[] parseStringToVector(String rawData)
  {

    String[] line = rawData.split("-");

    double[] vec = new double[line.length];

    for (int i =0 ; i < line.length ; i++)
    {
      vec[i] = Double.valueOf(line[i]);
    }

    return vec;

  }

  public void setindexID(int id)
  {
    this.indexID = id;
  }

  public int getIndexID()
  {
    return this.indexID;
  }

  public ArrayList<Integer> getLocalIndex()
  {
    return indices;
  }
}
