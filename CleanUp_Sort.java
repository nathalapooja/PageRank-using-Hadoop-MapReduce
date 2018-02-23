/*
 * Name: Pooja Reddy Nathala
 * ID: 800974452
 * pnathala@uncc.edu
 * */


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

// The main aim of this class is to find which pages have high ranks and sort them in descending order. The reducer generally sorts in ascending order which is modified using compare method to descending order.
//Intermediate data files are striped off and the final output file is remained with page ranks in descending order finally
public class CleanUp_Sort extends Configured implements Tool{

	
	
	public static class PRComparator extends WritableComparator{// PRComparator overrides the sort function of MapREduce which sorts the pages in descending order using the values of pageranks 
		
		protected PRComparator() {
			super(DoubleWritable.class,true);// rewrites the super class method 
		}
		
		@Override
		public int compare(WritableComparable value1, WritableComparable value2) {// compare() class sorts the ranks in descending order by returning the negative value of the compareTo() function
			DoubleWritable pr1 = (DoubleWritable) value1;// values are casted as DoubleWritable 
			DoubleWritable pr2 = (DoubleWritable) value2;
			
			int compare_value = pr1.compareTo(pr2);//The compareTo() method compares the Number object(pr1) that invoked the method to the argument(pr2)
			
			return -1 * compare_value;// retruns the negative value of compared value
		}
	}
	
	
	public static class CleanUp_SortMapper
	extends Mapper<Object, Text, DoubleWritable, Text>{// Mapper input: (page PageRank####-->PAGERANK<--####LinksList), where Links are disjointed by tag ####-->LINKEND<--####
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] Split_value = value.toString().split("\t");// value is splitted by delimiter "\t" and stored in Split_value
			String title = Split_value[0];// storing the page
			String pgRank = Split_value[1].split("####-->PAGERANK<--####")[0];//storing the page rank
			
			context.write(new DoubleWritable(new Double(pgRank)), new Text(title));// (pagerank,page) is written as (key,value) pair beacause the job output need pages to be ranked in descending order of their ranks
		}
		
	}
	
	
	public static class CleanUp_SortReducer
	extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{// Reducer INput: (pagerank, pages list) therefore the input key class:DoubleWritable and value class:Text
		
		
		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> Iterator_page = values.iterator();//The reducer uses the defined compareTo() method and sorts the pages in descending order
			
			while(Iterator_page.hasNext()){
				context.write(Iterator_page.next(), key);// Reducer Output: (page, pagerank) in descending order of page rank therefore the output key class:text and value class:DoubleWritable
			}
		}
		
	}
	
	/*
	 * CleanUp and  Sorting job Configuration
	 * Apart from Mapper and Reducer classes, Comparator class is defined
	 * By default, reducer sorts by key in ascending order
	 * 		To sort in descending order, this Comparator is defined
	 * */
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job_cleanUp;
		
		job_cleanUp = Job.getInstance(conf, "CLEANUP and Sorting");// Job new instance creation with name "CLEANUP and Sorting"
		job_cleanUp.setJarByClass(CleanUp_Sort.class);//Set the JAR to use, based on the class in use
		
		job_cleanUp.setMapperClass(CleanUp_SortMapper.class);//Set the Mapper class to defined CleanUp_SortMapper class
		job_cleanUp.setReducerClass(CleanUp_SortReducer.class);// Set the Mapper class to defined CleanUp_SortReducer class
		job_cleanUp.setSortComparatorClass(PRComparator.class);// here we set the comparator class as PRComparator class because the reducer sorts in increasing order by default
		
		
		job_cleanUp.setMapOutputKeyClass(DoubleWritable.class);// Sets the key class for the Mapper outptut data 
		job_cleanUp.setMapOutputValueClass(Text.class);// Sets the value class for the Mapper outptut data
		
		
		job_cleanUp.setOutputKeyClass(Text.class);// Sets the key class for the final outptut data 
	    job_cleanUp.setOutputValueClass(DoubleWritable.class);// Sets the key class for the final outptut data 
	    
	    job_cleanUp.setNumReduceTasks(1);// number of reduce tasks are set to 1
	    // This job takes the input declared in the first argument and stores the output in second argument
	    
	    FileInputFormat.addInputPath(job_cleanUp, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job_cleanUp, new Path(args[1]));
	    
	    return job_cleanUp.waitForCompletion(true) ? 0 : 1;// Lauch the job and wait for completion, returns 0 when the job is completed successfully otherwise 1
	}

}
