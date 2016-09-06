package AssignDataSetToGridCell;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.xerces.xs.datatypes.ByteList;
import org.mortbay.log.Log;
 


public class ReducerAlinePerCell extends Reducer<Text, Text, NullWritable, Text >{

	
	private MultipleOutputs<IntWritable,Text> twolevel;
	private MultipleOutputs<IntWritable,Text> index;
	
	RowKeyCreator rowKey; 
	@Override
	public void setup(Context context) 
	{
		this.twolevel =   new MultipleOutputs(context);
		
		this.index = new MultipleOutputs(context);
		
		this.rowKey = new RowKeyCreator(100000); // Needs twitching 25k//100k//..
	}
	@Override
 	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
 	{
		
 	   HashMap<String,Integer> localKey  = new HashMap<String,Integer>();

	   int count = 0;
	   
	   StringBuilder rowValues = new StringBuilder();
	   
	   for (Text value : values) 
		{
		   String point = value.toString();
		   
		   String localPartitionKey = rowKey.getAddress(point);
		   
		   if (localKey.containsKey(localPartitionKey))
		   {
			   localKey.put(localPartitionKey, localKey.get(localPartitionKey)+1);
			   
		   }
		   else
		   {
			   localKey.put(localPartitionKey,1);
		   }
		   
		   if (count==0)
		   {
			 	rowValues = new StringBuilder(point) ;
				count++;
			}
			else
			{
			    rowValues.append("\t"+ point);
			   count++;
			}
			             
		}
	    
	   context.write(NullWritable.get(), new Text(rowValues.toString()));
	    this.index.write(new IntWritable(count),key ,"heads"); //  index file 
	      // thing below is for 2level index - comment out for 1st level 	   
   //    for (String localkey : localKey.keySet())
      // {
	   // this.twolevel.write(new IntWritable(localKey.get(localkey)), new Text(key.toString() +"\t"+localkey),"Two-level-head");
     // }
      
        
	}
	@Override
 	protected void cleanup(Context context) throws IOException,InterruptedException
    {
		this.twolevel.close();
		this.index.close();
		
		 
	}
}