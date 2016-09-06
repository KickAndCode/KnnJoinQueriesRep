package quadTree;

import java.util.ArrayList;
import pythiaHbase.Helper;

public class Box {

  Point centre;

  double halfDimension;

  ArrayList<Point> points;

  int limit; 
  int total = 0;


  public Box()
  {

  }
  public Box(double x, double y, double halfDimension, int mlimit)
  {
    this.centre = new Point(x,y);

    this.halfDimension = halfDimension;

    this.limit = mlimit;

    points = new ArrayList<Point>();


  }

  public Box(double x, double y, double halfDimension )
  {
    this.centre = new Point(x,y);

    this.halfDimension = halfDimension;

    points = new ArrayList<Point>();

  }


  public Box(Point p, double halfDimension, int mlimit)
  {
    this.centre = p;

    this.halfDimension = halfDimension;

    this.limit = mlimit;

  }

  public boolean containsPoint(Point p)
  {


    return (p.getX() >= this.getMinX()&& p.getX() < this.getMaxX()
        && p.getY() >= this.getMinY()&& p.getY() < this.getMaxY());
  }

  public boolean intersect(Box b)
  {
    return (b.getMaxX() >= this.getMinX() && b.getMinX() <= this.getMaxX() && b.getMaxY() >= this.getMinY() && b.getMinY() <= this.getMaxY()) ||
        (b.getMaxX() <= this.getMinX() && b.getMinX() >= this.getMaxX() && b.getMaxY() <= this.getMinY() && b.getMinY() >= this.getMaxY());

  }

  public double getMinX()
  {
    return centre.getX()-halfDimension;
  }
  public double getMaxX()
  { 
    return centre.getX()+halfDimension;
  }
  public double getMinY()
  {
    return centre.getY()-halfDimension;
  }
  public double getMaxY()
  {
    return centre.getY()+halfDimension;
  }
  public boolean isEmpty()
  {
    return this.points.size() == 0;
  }
  public boolean isFull()
  {
    return this.points.size() == limit;
  }
  public boolean thereISSpace()
  {
    return (this.points.size() < limit) ;
  }

  public void addPoint(Point point)
  {		 
    points.add(point);
  }

  public Point getPoint()
  {
    return !isEmpty() ? this.points.remove(points.size()-1):null;
  }

  public Point getCentre() 
  {
    return centre;
  }

  public void setCentre(Point centre) 
  {
    this.centre = centre;
  }

  public double getHalfDimension()
  {
    return halfDimension;
  }
  public void setHalfDimension(double halfDimension) 
  {
    this.halfDimension = halfDimension;
  }
  public int getLimit()
  {
    return limit;
  }
  public void setLimit(int limit) 
  {
    this.limit = limit;
  }
  public int getSize()
  {
    return this.points.size();
  }

  public void print()
  {
    for (Point p : points)
    {
      System.out.println(p.getRowKey());
    }
  }

  public ArrayList<Point> getPoints()
  {
    return this.points;
  }

  public double getMaxDis(Point qry)
  {
    return Helper.MaxDis(new double[]{qry.getX(), qry.getY()}, new double[] {getMinX(),getMinY()}, 2*halfDimension);
  }


}
