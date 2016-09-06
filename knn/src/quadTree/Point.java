package quadTree;

public class Point {

	double x ;
	
	double y;
	
	int counter;
	
	String rowKey;
	
	Point()
	{
		
	}
	public Point(double x, double y, int count, String rowKey)
	{
	  this.x = x;
		this.y = y;
		this.counter = count;
		this.rowKey = rowKey;
	}
	
	public Point(double x, double y)
	{
	 
		this.x = x;
		this.y = y;
		 
	}


	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x=x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
	 	this.y = y;
	}
	public String getRowKey()
	{
		return this.rowKey;
	}
	public void setRowKey(String key)
	{
		this.rowKey = key;
	}
	public int getNumberOfDataInRow()
	{
		return this.counter;
	}
	


}
