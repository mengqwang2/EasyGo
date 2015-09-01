
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ApspImp extends Configured implements Tool{
	public static class ApspMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Node source = new Node(value.toString().trim());
			//Emit the exploded node
			for(Integer adj : source.getAdj()){
				Node adjacentNode = new Node(adj);
				
				adjacentNode.setAdjWeight(source.getId(), source.getAdjWeight(adj));
				
				for(Integer other : source.getAdj()){
					if(other != adj){
						adjacentNode.setAdjWeight(other, source.getAdjWeight(adj) + source.getAdjWeight(other));
					}
				}		
				context.write(new IntWritable(adj), adjacentNode.getFormattedValue(false));
			}

			// Emit input node with mark *.
			context.write(new IntWritable(source.getId()), source.getFormattedValue(true));
		}
	}
	
	public static class ApspReducer extends Reducer<IntWritable, Text, IntWritable, Text>
	{
			@Override
			public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
			{
				boolean converged = true;
				Node source = null;
				
				// cache iterated values
				Vector<Node> cache=new Vector<Node>();
				for(Text value : values)
				{
					if(value.charAt(value.getLength()-1)=='*')
					{
						source = new Node(key.get()+value.toString().substring(0, value.getLength()-2));
						break;
					}
					else
						cache.add(new Node(key.get()+value.toString()));
				}
				
				// check cached value first
				for(int i=0; i<cache.size(); ++i)
				{
					Node node = cache.get(i);
					for(Integer adj : node.getAdj())
					{
						if(node.getAdjWeight(adj) < source.getAdjWeight(adj))
						{
							source.setAdjWeight(adj, node.getAdjWeight(adj));
							converged = false;
						}
					}
				}
				
				cache = null;
				// continue with remained ones
				for(Text value : values)
				{
					Node node = new Node(key.get()+value.toString());
					//if(it==9 && key.get()==9)
						//System.out.println("Fuck lol 9\t" + node.getFormattedValue(false).toString());
					
					// compare with input source in map stage
					for(Integer adj : node.getAdj())
					{
						if(node.getAdjWeight(adj) < source.getAdjWeight(adj))
						{
							source.setAdjWeight(adj, node.getAdjWeight(adj));
							converged = false;
						}
					}
					
					node = null;
				}
				
				converged = converged && (source.getAdjSize()==context.getConfiguration().getInt("graphSize", 0)-1);
				// write to output
				context.write(key, source.getFormattedValue(converged));			
				
				
				source = null;
			}
	}
	
	private boolean allNodeConverged(String dir) throws Exception
	{
		if( dir == null )
			return false;
		
		int cnt=0;
		
		FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(dir));
        for (int i=0;i<status.length;i++){
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line = br.readLine();
                while (line != null)
                {
                	if( line.endsWith("*") )
                		++cnt;
                	line=br.readLine();
                } 
        }
        return cnt == this.getConf().getInt("graphSize", 0);
	}
	
	public int run(String args[]) throws Exception {
		String fileInputPath=args[2],fileOutputPath=null;
		int iteration = 0;
		Configuration conf = this.getConf();
		conf.setInt("graphSize", Integer.parseInt(args[1]));
		
		long startTime = System.currentTimeMillis();
		
		while( !allNodeConverged(fileOutputPath) ){
			if(iteration > 0)
				fileInputPath = "output-"+iteration;
			
			fileOutputPath = "output-"+(iteration+1);
			
			
			Job job = Job.getInstance(this.getConf());
			job.setJobName("newAPSP");
			//job.setJarByClass(ApspImp.class);
			job.setJar("test.jar");
			job.setMapperClass(ApspMapper.class);
			job.setReducerClass(ApspReducer.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(fileInputPath));
			FileOutputFormat.setOutputPath(job, new Path(fileOutputPath));
			
			if( !job.waitForCompletion(true) )
			{
				System.err.println("Iteration " + (iteration+1) + " failed!");
				System.exit(1);
			}
			
			job.getCounters();	
			
			++iteration;
		}
		
		// Print the test statistics
		double timeElapsedSinceStartInSeconds = ((double) (System.currentTimeMillis() - startTime)) / 1000;

		System.out.println("Total Time for All pairs shortest path execution: " + timeElapsedSinceStartInSeconds);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		int rtn = ToolRunner.run(new ApspImp(), args);
		System.exit(rtn);
	}
}
