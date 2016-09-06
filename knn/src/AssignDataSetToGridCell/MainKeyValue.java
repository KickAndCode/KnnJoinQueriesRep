package AssignDataSetToGridCell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
 
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainKeyValue extends Configured implements Tool {	
        
    
    public static void main(String[] args) {		
        try 
        {
        	
            int response = ToolRunner.run( new MainKeyValue(), args);	
            
            if(response == 0) 
            {				
                System.out.println("Job is successfully completed...");
            } 
            else 
            {
                System.out.println("Job failed...");
            }
        } 
        catch(Exception exception) 
        {
            exception.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        int result=0;
        String input = "hdfs://localhost:9000/user/input/200kDataset";
       // String input = "/home/shadoop/Data/qry10kIndexed";//for second level index        
       String output = "hdfs://localhost:9000/user/output/200kw100kIndexed"; //  the folder of Data + Index( head ) 
        // String output = "/home/atoshum_user/test8978";
        
        Path outputPath = new Path(output);
        
        
        
        Configuration configuration = getConf();
       configuration.set("mapred.child.java.opts", "-Xmx8024m");
       configuration.set("dfs.replication", "1");
        configuration.setBoolean("mapred.output.compress", false);
        configuration.addResource(new Path("/home/shadoop/hadoop-2.7.2/conf/core-site.xml"));
        configuration.addResource(new Path("/home/shadoop/hadoop-2.7.2/conf/hdfs-site.xml"));
        
        
        FileSystem hdfs = FileSystem.get(configuration);
        
       // if (hdfs.exists(outputPath))  hdfs.delete(outputPath, true);
             	
        Job job = Job.getInstance(configuration);
             
        job.setJobName("A Line Per Cell");
        
        FileInputFormat.addInputPaths(job, input); // use this line for single index
        
       // FileInputFormat.addInputPath(job, new Path(input+ "/" + "head-r-" + "*")); // use this line for second index
       
        FileOutputFormat.setOutputPath(job, outputPath);
                           
        job.setJarByClass(getClass());
        
        job.setMapperClass(MapperAlinePerCell.class);
        job.setReducerClass(ReducerAlinePerCell.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	   
	    
	    
	    
        job.waitForCompletion(true);
        
         
    
        
        return result;
    }
}