package bulkLoad;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
//import org.apache.hadoop.hbase.HBaseCluster;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkLoadingMain extends Configured implements Tool {	
    private static final String DATA_SEPERATOR = ",";	
    private static final String TABLE_NAME = "200kW100k";	
   // private static final String TABLE_NAME = "PythiaTest";	
    private static final String COLUMN_FAMILY="C";	
//    private static final String prefix = "/user/atoshum/pythiaGridIndex/";
//    private static final String postfix = "-r-00000";
     
   // private static final String COLUMN_FAMILY_2="contactDetails";	
    /**
     * HBase bulk import example
     * Data preparation MapReduce job driver
     * 
     * args[0]: HDFS input path
     * args[1]: HDFS output path
     * 
     */
    public static void main(String[] args) {	
    
        try 
        {
        	
            int response = ToolRunner.run(HBaseConfiguration.create(), new BulkLoadingMain(), args);	
            
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
        
       // String input  = prefix+args[0]+postfix;
       // String input = "/user/atoshum/CellPerLine";
       // String input = "/home/shadoop/Data/qry10kIndexed/10kQry-head-r-00000";        
        String input="hdfs://localhost:9000/user/output/200kw100kIndexed";
        String output = "hdfs://localhost:9000/user/output/200kHbase/";
        
             
        Configuration configuration = getConf();
        configuration.addResource(new Path("/home/shadoop/hadoop-2.7.2/conf/core-site.xml"));
        configuration.addResource(new Path("/home/shadoop/hadoop-2.7.2/conf/hdfs-site.xml"));
        
        FileSystem hdfs = FileSystem.get(configuration);
        
        
      //  if (!hdfs.exists(inputPath))  {System.out.print("no input"); return -1;}
        
        Connection connection = ConnectionFactory.createConnection(configuration);
          
        HBaseConfiguration.addHbaseResources(configuration);
        
        configuration.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 64);
        
        configuration.set("mapred.child.java.opts", "-Xmx4g");
       
        configuration.set("dfs.replication", "1");
                	
        Job job = Job.getInstance(configuration);
             
        job.setJobName("Bulk Loading : "+TABLE_NAME);
        Path outputPath = new Path(output);
        
        
       // if (hdfs.exists(outputPath))  hdfs.delete(outputPath, true);
        FileInputFormat.addInputPath(job, new Path(input+ "/" + "part" + "*"));
        //FileInputFormat.addInputPath(job, inputPath);
       
        //FileSystem.getLocal(getConf()).delete(outputPath, true);		
        
        FileOutputFormat.setOutputPath(job, outputPath);
       
        
        job.setJarByClass(getClass());
        
        job.setMapperClass(MapperCreateGrids.class);
        
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    
        job.setMapOutputValueClass(KeyValue.class);
	    
        job.setOutputFormatClass( HFileOutputFormat.class);
	   
	    
                
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
     	
        HFileOutputFormat.configureIncrementalLoad(job,(HTable) table);
        
        
     
       job.waitForCompletion(true);
        
      if (job.isSuccessful())
      {
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
                      
        loader.doBulkLoad(outputPath, (HTable) table );
        
        // FileSystem.getLocal(getConf()).delete(outputPath, true);
      }
      else
      {
    	  result = -1;
      }
        
        return result;
    }
}