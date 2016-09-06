package pythiaHbase;



public class DataDistance {


  double dis;	double[] vector;	String rowKey;		int numDataItems;		 int indexID;



  public DataDistance(double[] vec, double dis)
  {
    this.dis = dis;
    this.vector = vec;
  }

  public DataDistance(double[] vec, double dis, int numData)
  {
    this.dis = dis;
    this.vector = vec;

    this.numDataItems = numData;
  }

  public DataDistance(String path, double dis, int numData, double[] vec, int IndexID)
  {
    this.dis = dis;
    this.rowKey = path;
    this.numDataItems = numData;
    this.vector = vec;
    //this.indexID = indexID;
  }

  public DataDistance(String path, double dis, int numData, double[] vec)
  {
    this.dis = dis;
    this.rowKey = path;
    this.numDataItems = numData;
    this.vector = vec;

  }


  public DataDistance( double dis)
  {
    this.dis = dis;

  }

  public Double getDistance() {
    return dis;
  }

  public void setDistance(Double distance) {
    this.dis = distance;
  }

  public double[] getVector() {
    return vector;
  }

  public void setVector(double[] vector) {
    this.vector = vector;
  }

  public String vecDistanceString()
  {
    String x = "";
    for (double d : vector)
    {
      x+= String.valueOf(d)+",";
    }
    x+= " "+String.valueOf(dis);
    return x;
  }

  public String getRowKey()
  {

    return this.rowKey;
  }

  public double getMinDis() {
    return dis;
  }
  public void setMinDis(double minDis) {
    this.dis = minDis;
  }
  public int getNumDataItems() {
    return numDataItems;
  }
  
  public void setVectorString(String vectorString) {
    this.rowKey = vectorString;
  }

  public int getIndexID()
  {
    return this.indexID;
  }

}
