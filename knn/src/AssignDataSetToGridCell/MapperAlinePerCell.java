package AssignDataSetToGridCell;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public   class MapperAlinePerCell extends Mapper<LongWritable, Text,Text, Text> {
     
	 RowKeyCreator rowKey;
	public void setup(Context context) 
	{
		 rowKey = new RowKeyCreator(100000); // 300 //25k// 100k-> width of cell 
			  
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{         
		String firstLeveIndex = rowKey.getAddress(value.toString());
		//String seconLevelIndex = rowKey.getSecondLevelAddress(value.toString());
		
		//if (seconLevelIndex  != "")
		if(firstLeveIndex !="")
		{
		     context.write(new Text(firstLeveIndex), value);
		     // context.write(new Text(seconLevelIndex), new Text(value.toString().split("\t")[1]));
		}
    }
 
	






}