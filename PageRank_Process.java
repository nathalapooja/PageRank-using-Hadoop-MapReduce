/*
 * Name: Pooja Reddy Nathala
 * ID: 800974452
 * pnathala@uncc.edu
 * */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

// This class runs the pagerank algorithm and outputs the page,its corresponding page rank , list of pages to which it is connected to 
public class PageRank_Process extends Configured implements Tool{

        @Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job_processPR;
		
		job_processPR = Job.getInstance(conf, "Page Rank Process");// Job new instance creation with name "Page Rank"
		job_processPR.setJarByClass(PageRank_Process.class);//Set the JAR to use, based on the class in use
	        job_processPR.setMapperClass(PageRank_ProcessMapper.class);//Set the Mapper class to defined PageRank_ProcessMapper class
	        job_processPR.setReducerClass(PageRank_ProcessReducer.class); // Set the Reducer class to defined PageRank_ProcessReducer class
	        job_processPR.setOutputKeyClass(Text.class);// Sets the key class for the outptut data
	        job_processPR.setOutputValueClass(Text.class);// Sets the Value class for the outptut data
	        // This job takes the input declared in the first argument and stores the output in second argument
	        FileInputFormat.addInputPath(job_processPR, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job_processPR, new Path(args[1]));
	    
	    return job_processPR.waitForCompletion(true) ? 0 : 1;// Lauch the job and wait for completion, returns 0 when the job is completed successfully otherwise 1
	}

	
	public static class PageRank_ProcessMapper
	extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			List<String> linklist = new ArrayList<String>();
			
			//To seperate title and links from the values by iterating through each value 
			String[] indata = value.toString().split("\t");
			String title = indata[0];// the first split is stored as title
			
			//To seperate initial pagerank and list of links 
			String[] SplitforLink = indata[1].split("####-->PAGERANK<--####");
			double initialPageRank = Double.parseDouble(SplitforLink[0]);
			
			//check for list of links, if the links are present, split them using the tag and divide the initialPageRank among the list of links
			if(SplitforLink.length > 1){
				linklist = Arrays.asList(SplitforLink[1].split("####-->LINKEND<--####"));
				
				double rankShare = initialPageRank / linklist.size();
				
				for (String link : linklist) {
					context.write(new Text(link), new Text(String.valueOf(rankShare)));// each link , its corresponding page rank share is written as intermediate (key,value) pair to the context object for the job
				}
				context.write(new Text(title),new Text("<PAGELINKS>" + SplitforLink[1] + "</PAGELINKS>"));// title and the list of links to which it is pointed is written as intermediate (key,value) pair to the 
                                                                                                                              //context object for the job
			} 
			//else we are writing just the title and tag as (key,value) pair
			else{
				context.write(new Text(title), new Text("<PAGELINKS></PAGELINKS>"));//writes a intermediate key/value pair to the context object for the job
			}
			
			
		}
		
	}
	
	public static class PageRank_ProcessReducer
	extends Reducer<Text, Text, Text, Text>{
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)// Reducer takes input in the form:(page(node),page rank shares list form each page and list of links outgoing from it(current page)
				throws IOException, InterruptedException {
			final double d = 0.85;// defining the damping factor as 0.85
			double prWithoutd = 0;// we are initially setting the pagerank without damping factor to 0
			
			boolean isTitle = false;//isTitle is initially set to false so that when title is matched it is set to true
			
			String linklist = "";
			Iterator<Text> valueIterator = values.iterator();
			
			//Pattern which used to identify the list of links 
			Pattern lPattern = Pattern.compile("<PAGELINKS>(.*?)</PAGELINKS>");
			
			while(valueIterator.hasNext()){//This loop runs until the valueIterator has no next value
				String value = valueIterator.next().toString();
				Matcher lMatch = lPattern.matcher(value);// if the match is found to the pattern then the isTitle value is set to true
				
				//If the links are present they are grouped into single varaible
				if(lMatch.find()){
					isTitle = true;
					
					linklist = lMatch.group(1);
				} 
				//Else the page rank shares without including the damping factor in the values iterator is aggregated 
				else{
					prWithoutd += Double.parseDouble(value);
				}
				
			}
			
			
			 //Final PageRank is calculated as:PageRank = (1-d) + d*(aggregatedValue), where d is a damping factor taken as 0.85
                         //damping factor defines the probability of going from one url to other urls to which the current page url is pointing
                         // 1-d=0.15 defines the probability of jumping to random urls from current url
			double PageRank_final = 0.15 + (0.85 * prWithoutd);
			
			if(isTitle)
				context.write(key, new Text(PageRank_final + "####-->PAGERANK<--####" + linklist));//output : page	PageRank####-->PAGERANK<--####LinksList	where Links are disjointed by tag ####-->LINKEND<--#### is 
                                                                                                                    //written to the context object for the job
		}
	
	}
	
	
	
	

}
