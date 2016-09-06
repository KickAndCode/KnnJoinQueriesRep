package pythiaHbase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

public class FirstLayerGridIndex
{
	 
	private ArrayList<CellSummary>  signatures;
		
	private double cellWidth;
	
    private int k;
    
    AscendingHeapSort candidates;
       
    private HashMap<String, Integer> set;
    
    
	
    
	

	public FirstLayerGridIndex( double cellWidth, int k)  
	{
		this.signatures = new ArrayList<CellSummary>();
			
		this.cellWidth = cellWidth;
		
		this.k = k;
			
		set = new HashMap<String,Integer> ();
	}
	
	/*
	 * 
	 */
	public   String createIndexCoordinateID(double[] vec, int interval)
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
	
	public  String getAddress(double point, int interval)
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
	
	/*
	 * input: x and y coordinates of a point (vec), which is a point in the second layer index
	 * index (int) is the index of the point on the second layer signature
	 * numItes: count of points that are represented by this particular second layer index cell  
	 * 
	 */
	public  void constructIndex(double[] vec, int index, int numItems ) throws IOException
	{
		String aPointOnlayerOneIndex =  createIndexCoordinateID(vec, (int) cellWidth);
				
		if (set.containsKey(aPointOnlayerOneIndex ))
		{
			 
			int i = set.get(aPointOnlayerOneIndex);
				 
			signatures.get(i).addDataToACell(index, numItems); 
        
		}
		
		else
		{
			signatures.add(new CellSummary(aPointOnlayerOneIndex,numItems, index));
			
			set.put(aPointOnlayerOneIndex, signatures.size()-1);
			
			 
		}
	}
	
	
	/*
	 * input: x and y coordinates of a point (query) 
	 * action: sorts  first layer cells based on their distance to the query 
	 * output: ascending heap sort - first layer cells sorted from the closest to the farest 
	 */
	
	public  AscendingHeapSort sortIndexBasedDistToQry( double[] qry )
	{
		AscendingHeapSort hps = new AscendingHeapSort();
		        
		for (int i =0 ; i < signatures.size(); i++)
		{
			 
			hps.addNewData( signatures.get(i).getRowKey(), Helper.minDis(qry, this.signatures.get(i).getVec(),
					this.cellWidth),this.signatures.get(i).getNumberOfDataItems(), this.signatures.get(i).getVec(),i);

			 
		}
		return hps;
	}
	
	/*
	 * input: x and y coordinates of a point (query) 
	 * output: the index of the second layer candidate cells
	 * 
	 */
	
	public ArrayList<Integer> getCandidates(double[] qry) throws IOException, URISyntaxException
	{
      	AscendingHeapSort candidates = sortIndexBasedDistToQry(qry);

		int totalNumberOFK = 0;						   

		double maxDis = Double.POSITIVE_INFINITY;	  

		boolean enoughK = false;

		ArrayList<Integer> secondLayerCandIndexID = new ArrayList<Integer>();
 
		while (!candidates.isEmpty())
		{
                   
			DataDistance cand = candidates.removeMin();

			totalNumberOFK += cand.getNumDataItems();

			if (! enoughK && totalNumberOFK >= k)
			{
				enoughK = true;
                 
			     maxDis =  Helper.MaxDis(qry, cand.getVector(), cellWidth);
				   
			     
				 
			}
			if ( cand.getMinDis() <= maxDis ) 
			{  
				/*
				 * the following loop inserts all second level cell that are located within the maximum distance that is calculated based on 
				 * the width of the first layer cell width. 
				 * Optimising the max-distance to a much tighter max-distance reduces the size of array that contains second layer candidates 
				 */
               for (int c : signatures.get(cand.getIndexID()).getIndices())
               {
            	  
            	   secondLayerCandIndexID.add(c);
               }

				
			}
			else
			{
				break;
			}



		}
		 

		return secondLayerCandIndexID;
 }
	
	public void setK(int k)
	{
		this.k = k;
		
		 
	}
	
	public int getSigSize()
	{
		return this.signatures.size();
	}
	 
	
 
		 
	
	
	
	



}
