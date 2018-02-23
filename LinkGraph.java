/*
 * Name: Pooja Reddy Nathala
 * ID: 800974452
 * pnathala@uncc.edu
 * */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Iterators;



public class LinkGraph extends Configured implements Tool {

	/*
         * This LinkGraph class parses the given input , counts number of documents in the given input ,
         * and creates the link graph and sets the initial pagerank to each node page using the results of parsed input
         */ 
	
	public static class LinkGraphMapper
    extends Mapper<Object, Text, Text, Text>{//Map class parses the each line of the input data and combines identified title and links using delimiters
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String document = value.toString();// each line of input corresponds to one document 
			List<String> list_links = null;
			
			Pattern tPattern = Pattern.compile("<title>(.*?)</title>");// a regular expression pattern you can use to parse each line of input text on word boundaries ("<title>(.*?)</title>").
			Matcher tMatcher = tPattern.matcher(document);//matcher method attempts to match the entire input sequence against the defined pattern. 
			
			Pattern lPattern = Pattern.compile("(\\[{2})(.*?)(\\]{2})");// a regular expression pattern you can use to parse each line of input text on word boundaries ("(\\[{2})(.*?)(\\]{2})").
			Matcher lMatcher = lPattern.matcher(document);// matcher method attempts to match the entire input sequence against the defined pattern. 
			
			if(tMatcher.find()){// Input data lines without title tag is not taken into account
				list_links = new ArrayList<String>();
				
				
				while(lMatcher.find()){
					String link = lMatcher.group(2);//Returns the subsequence of input captured by the given group during the previous match operation.
					

					if(!link.contains("[["))
						list_links.add(link.replace('\t', ' '));// spaces replaces the tabs
				}
				
				
				String mapValue = tMatcher.group(1).replace('\t', ' ')
							+ "<<<<----####---->>>>" + StringUtils.join(list_links,"####-->LINKEND<--####");
				
				context.write(new Text("Key"), new Text(mapValue));//writes a intermediate key/value pair to the context object for the job.
                                                                                   // Mapper Value structure:Page<<<<----####---->>>>Links_from_Page(where  ####-->LINKEND--#### is used to seperate links)
                                                                                   
			}
			
		}
	}
	
	public static class LinkGraphReducer
    extends Reducer<Text, Text, Text, Text>{// This reducer counts the number of documents and calculates initial page rank (1/Number of documents) 
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {// Input to the Reducer: ("Key", Page<<<<----####---->>>>Links_from_Page), where Links are separated by ####-->LinkGraph<--####
			
			Iterator<Text> dIterator = values.iterator();// text iterator is created to iterate over values list
			List<String> data = new ArrayList<String>();
			
			int NoOfDocs = 0;
			
			//Number of documents are counted by iterating through the values of particular key
			while(dIterator.hasNext()){
				data.add(dIterator.next().toString());
				
				NoOfDocs++;
			}
			
			Double init_PR= (1.0/NoOfDocs);// Initial page Rank= 1/Number of Documents
			
			
			for (String stg : data) {
				String[] docs = stg.split("<<<<----####---->>>>");
				
				Text reduceKey = new Text(docs[0]);//reducer key is taken as first string of docs list which has the page title
				Text reduceValue;
				
				if(docs.length == 2)
					reduceValue = new Text(init_PR+"####-->PAGERANK<--####"+docs[1]);
				else reduceValue = new Text(init_PR+"####-->PAGERANK<--####");// Pages without links are valid pages, which are not considered while calculating the pagerank 
				
				context.write(reduceKey, reduceValue);//writes a  key/value pair to the context object for the job.
                                // Output format for the Reducer: (Page , initial_PageRank####-->PAGERANK<--####Links_from_Page) , where Links are separated by ####-->LinkGraph<--####
			}
		}
	}
	
	
	/*
	  * Reducer count is set to 1 to count the total number of valid pages in the corpus given. 
	 * */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job_linkgraph;
		
		job_linkgraph = Job.getInstance(conf, "Link Graph");// Job new instance creation with name "Link Graph"
		job_linkgraph.setJarByClass(LinkGraph.class);//Set the JAR to use, based on the class in use
	    job_linkgraph.setMapperClass(LinkGraphMapper.class);// Set the Mapper class to defined LinkGraphMapper class
	    job_linkgraph.setReducerClass(LinkGraphReducer.class);//Set the Mapper class to defined LinkGraphReducer class
	    
	    job_linkgraph.setOutputKeyClass(Text.class);// Sets the key class for the outptut data
	    job_linkgraph.setOutputValueClass(Text.class);// Sets the value class for the outptut data
	    
	    job_linkgraph.setNumReduceTasks(1);// number of reduce tasks are set to 1
	    // This job takes the input declared in the first argument and stores the output in second argument

	    FileInputFormat.addInputPath(job_linkgraph, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job_linkgraph, new Path(args[1]));
		
	    return job_linkgraph.waitForCompletion(true) ? 0 : 1;// Lauch the job and wait for completion, returns 0 when the job is completed successfully otherwise 1
	}

}
