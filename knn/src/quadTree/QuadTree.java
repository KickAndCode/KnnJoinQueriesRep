package quadTree;

import java.util.ArrayList;

import pythiaHbase.AscendingHeapSort;
import pythiaHbase.Helper;

public class QuadTree 
{
	private final Box mbr;

	QuadTree northWest,  northEast ,  southWest ,  southEast = null;
	int width;
	int limit; ArrayList<Point> points; int total = 0;
	String name = "";


	public QuadTree(double x, double y, double halfDimension, int limit, int mbrWidth)
	{
		this.mbr = new Box(x, y, halfDimension, limit);
		this.limit = limit;
		this.width = mbrWidth;
	}

	public void subDivide()
	{
		//double divider = 2.0;
		double mhalf = this.mbr.halfDimension / 2.0;
		northEast = new QuadTree(this.mbr.getCentre().getX()+mhalf, this.mbr.getCentre().getY()+mhalf, mhalf, this.limit, this.width);
		northEast.set("ne");
		northWest = new QuadTree(this.mbr.getCentre().getX()-mhalf, this.mbr.getCentre().getY()+mhalf, mhalf, this.limit, this.width);
		northWest.set("nw");
		southEast = new QuadTree(this.mbr.getCentre().getX()+mhalf, this.mbr.getCentre().getY()-mhalf, mhalf, this.limit, this.width);
		southEast.set("se");
		southWest = new QuadTree(this.mbr.getCentre().getX()-mhalf, this.mbr.getCentre().getY()-mhalf, mhalf, this.limit, this.width);
		southWest.set("sw");

		while (!this.mbr.isEmpty())
		{

			Point p  = this.mbr.getPoint();
			if (p != null)  addData(p) ;
		}


	}

	private boolean addData(Point p)
	{
		return (northWest.insert(p) || northEast.insert(p) || southWest.insert(p) || southEast.insert(p) );
	}
	public boolean insert(Point p)
	{
		if  (!mbr.containsPoint(p)) return false;

		if  (mbr.thereISSpace() && this.northWest ==null )
		{
			mbr.addPoint(p);
			return true;
		}

		if (this.northWest == null )	this.subDivide();

		return addData(p);

	}


	public void getCanIndexRange(Box b, AscendingHeapSort pointsInRange, Point qry)
	{
	  
		if (! mbr.intersect(b)) return  ;

		if (northWest == null )
		{
			for (Point p :  mbr.points)
			{

				if (b.containsPoint(p)) 
				{
				  double dis = Helper.minDis(new double[] {qry.getX(), qry.getY()}, new double[] {p.getX(), p.getY()}, this.width);
					pointsInRange.addNewData(p.getRowKey(),dis,p.getNumberOfDataInRow(), new double [] {p.getX(), p.getY()},0);
				}
			}
			return;
		}

		northEast.getCanIndexRange(b, pointsInRange, qry);
		northWest.getCanIndexRange(b, pointsInRange, qry);
		southWest.getCanIndexRange(b, pointsInRange, qry);
		southEast.getCanIndexRange(b, pointsInRange, qry);

	}

	public void set(String name)
	{
		this.name = name;
	}
	public String getName()
	{

		return	this.name ;

	}


	public void  getCandidateRegion(Point qry,  Box winner)
	{
	  
		if (!mbr.containsPoint(qry)) return ;
		if (northWest == null )
		{
			winner.setCentre(mbr.getCentre());
			
			winner.setHalfDimension(mbr.getMaxDis(qry));
			return ;
		}
		northEast.getCandidateRegion(qry, winner);
		northWest.getCandidateRegion(qry, winner);
		southWest.getCandidateRegion(qry, winner);
		southEast.getCandidateRegion(qry, winner);

	}

	public void leafNode()
	{
		if (northWest == null )
		{

			System.out.println("------------------");
			System.out.println(mbr.getMinX() +"-"+mbr.getMaxX());
			System.out.println(mbr.points.size());
			System.out.println("------------------");


			return;
		}
		northEast.leafNode();
		northWest.leafNode();
		southWest.leafNode();
		southEast.leafNode();
	}

}
