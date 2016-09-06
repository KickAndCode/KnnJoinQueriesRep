package bulkLoad;
import java.io.IOException;
import java.util.HashMap;


import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 



public   class MapperCreateGrids extends Mapper<LongWritable, Text,ImmutableBytesWritable, KeyValue> {
     
	
 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	   String myKey = getAddress(value.toString().split("\t")[0]);
	   
	   ImmutableBytesWritable hKey = new ImmutableBytesWritable(Bytes.toBytes(myKey));
	   
	   KeyValue kv = new KeyValue(hKey.get(), Bytes.toBytes("C"), Bytes.toBytes("x"), Bytes.toBytes(value.toString()));
	   
	   context.write(hKey, kv);
        
    }
 
	public String getAddress(String vec)
	{
		String addr = "";
		String[] vec1 = vec.split(",");
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
	
	public   String address(double point)
	{
		int interval = 100000; //Width of cell ! needs to be edited 
				
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