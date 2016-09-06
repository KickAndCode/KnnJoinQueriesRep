package pythiaHbase;

public class  Helper 
{
  public static double minDis(double[]qry, double[] rect, double cellWid)
  {

    double sum =0;
    for (int i = 0 ; i < qry.length ; i++)
    {
      double border = -1;
      if (Double.compare(qry[i], rect[i])<0)
      {
        border = rect[i];
      }
      else if (Double.compare(qry[i], (rect[i]+cellWid))>0)
      {
        border = rect[i]+cellWid;
      }
      else
      {
        border = qry[i];
      }

      sum += Math.pow((qry[i] - border),2);


    }
    return Math.sqrt(sum);
  }

  public static double rmk (double p, double s, double t)
  {
    if (Double.compare((s + t)/2.0, p)<=0) return s;
    return t;
  }

  public static double rMi (double p, double s, double t)
  {
    if (Double.compare(p,(s + t)/2.0)>=0) return s;
    return t;
  }

  public static double MaxDis(double[] qry, double[] rect, double cellWid)
  {
    double sum = 0.0;
    for (int k = 0 ; k < qry.length ; k++)
    {

      double localsum = Math.pow((qry[k] - rmk (qry[k], rect[k], rect[k]+cellWid)), 2);

      for (int i =0 ; i < qry.length; i++)
      {
        if (k == i) continue;

        localsum += Math.pow(( qry[i] -rMi(qry[i], rect[i], rect[i]+cellWid) ) , 2);
      }

      if ( Double.compare(sum, localsum) < 0) sum = localsum;

    }

    return Math.sqrt(sum);

  }

  public static double[] parseStringToVector(String rawData)
  {
    String[] line = rawData.split(",");
    double[] vec = new double[line.length];
    for (int i =0 ; i < line.length ; i++)
    {
     
        vec[i] = Double.valueOf(line[i]);
    
    }


    return vec;

  }

  public static double[] parseStringToVector1(String rawData) 
  {
    String[] line = rawData.split("-");
    double[] vec = new double[line.length];
    for (int i =0 ; i < line.length ; i++)
    {
      vec[i] = Double.valueOf(line[i]);
    }

    return vec;

  }


  public static String convertDoubleToString(double[] vec)
  {
    int len = vec.length;
    String rowKey = "";
    for (int i = 0 ; i < len ; i++)
    {
      if (i == 0)
      {
        rowKey = String.valueOf(vec[i]);			
      }
      else
      {
        rowKey += "-"+String.valueOf(vec[i]);
      }

    }
    return rowKey;

  }

  public  static String createIndexCoordinateID(double[] vec, int interval)
  {
    String id= "";

    for (int i = 0; i < vec.length; i++)
    {
      if (id == "")
      {
        id = getAddress( vec[i], interval);
      }
      else
      {
        id += "-"+getAddress( vec[i], interval);
      }
    }
    return id;
  }

  public  static String getAddress(double point, int interval)
  {


    int lowerBoundry = (int) (point / interval);

    if (lowerBoundry==0)
    {
      return String.valueOf(lowerBoundry);
    }
    else
    {
      return String.valueOf(lowerBoundry * interval);
    }

  }





}
