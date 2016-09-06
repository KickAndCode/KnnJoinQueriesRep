package AssignDataSetToGridCell;

public class RowKeyCreator 
{
	int interval;
	public RowKeyCreator(int interval)
	{
		this.interval = interval;
	}
	
	public String getAddress(String vec)
	{
		String addr = "";
		String[] vec1 = vec.split(",");
		 
		if (vec1.length != 2) return "";
		
		for (int i = 0; i < vec1.length; i++)
		{
			if (addr == "")
			{
				addr = address(Double.valueOf(vec1[i]));
			}
			else
			{
				addr += "-"+address(Double.valueOf(vec1[i]));
			}
		}
		return addr;
	}
	
	public String address(double point)
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
	
	public String getSecondLevelAddress(String vec)
	{
		String addr = "";
		String[] vec1 = vec.split("\t")[1].split("-");
		 
		if (vec1.length != 2) return "";
		
		for (int i = 0; i < vec1.length; i++)
		{
			if (addr == "")
			{
				addr = address(Double.valueOf(vec1[i]));
			}
			else
			{
				addr += "-"+address(Double.valueOf(vec1[i]));
			}
		}
		return addr;
	}
	

}
