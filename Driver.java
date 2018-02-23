/*
 * Name: Pooja Reddy Nathala
 * ID: 800974452
 * pnathala@uncc.edu
 * */


import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/*
 * The Main class is the Driver class which contains only one main function and which calls all other modules 
 * Main function has 3 arguments: InputPath, OutputPath and number of iterations
 * Main function runs jobs in the following order
 * 		1. LinkGraph - to perform document count from XML representation in the input corpus and formatting pages with initial page rank 1/N and corresponding links 
 * 		2. PageRank_Process - runs the PageRank algorithm with the given number of iterations
 *       	3. CleanUp_Sort - removes all outlinks and extracts only Page with its corresponding rank
 * */

public class Driver {
	
	public static void main(String[] args) throws Exception {
		Driver D = new Driver();
		String OutputPathFinal = args[1];//OutputPathFinal is used to store only final outputs
		
                String[] OutputPathsplit = OutputPathFinal.split("/");// Intermediate output paths are generated with name InterOutFiles to store intermediate outputs like job1 output
		int outputPathLength = OutputPathsplit.length;
		OutputPathsplit[OutputPathsplit.length-1] = "InterOutFiles";
		String interOutPath = StringUtils.join(OutputPathsplit, "/");
			
		String[] job1Arguments = {args[0],interOutPath}; //input path and intermediate output path are given to 
		
                // Here we are running the first job LinkGraph
                 // Firstly, The main method invokes LinkGraphToolRunner , which creates and runs a new instance of LinkGraph, passing the command line arguments(job1Arguments).
		int LinkGraphToolRunner = ToolRunner.run(new LinkGraph(), job1Arguments);//JOB1-generates the link graph which performs document count, find initial pagerank and genrates the graph with links between nodes

	    
	    //Here we are running the pagerank job for the given number of iterations stored in noOfIterations
	    int noOfIterations = Integer.parseInt(args[2]) + 1;
	    String[] job2Arguments = new String[2];
	    job2Arguments[0] = interOutPath;// Input path is the output path of the link Grapgh job
	    
	    for(int i = 1; i < noOfIterations; i++){ 
	    	OutputPathsplit[outputPathLength-1] = "ItrOut" + i ;
	    	String OutPathOFPageRank = StringUtils.join(OutputPathsplit, "/");// The intermediate output path name is designed as ItrOut1 for i=1 simailarly for i=2,3..  upto n
	    	
	    	job2Arguments[1] = OutPathOFPageRank;
                // Secondly, The main method invokes pageRankToolRunner , which creates and runs a new instance of PageRank, passing the command line arguments(job2Arguments).
	    	int pageRankToolRunner = ToolRunner.run(new PageRank_Process(), job2Arguments);
	    	
	    	if(i == 1)
	    		D.DirectoryDelete(interOutPath);// For first iteration linkGraph output is deleted
	    	else {
	    		OutputPathsplit[outputPathLength-1] = "ItrOut" + (i-1) ; // From second iteration ItrOut i-1 path file is deleted
	    		String DeletingPath = StringUtils.join(OutputPathsplit, "/");
	    		D.DirectoryDelete(DeletingPath);
	    	}
	    	
	    	job2Arguments[0] = OutPathOFPageRank;// Input path to the next iteration is the output of page rank job from previous iteration
	    }
	    
	    
	    //Here we are running the CleanUp Job which cleans ItrOutn-1 output folder and stores the final list of pages in descending order into the output folder path
	    
	    String[] job3Arguments = new String[2];
	    job3Arguments[0] = job2Arguments[0];
	    job3Arguments[1] = args[1];
	    // Thirdly, The main method invokes cleanUpToolRunner , which creates and runs a new instance of CleanUp, passing the command line arguments(job3Arguments).
	    int cleanUpToolRunner = ToolRunner.run(new CleanUp_Sort(), job3Arguments);
	    
	    D.DirectoryDelete(job3Arguments[0]);// ItrOut n-1 path file is deleted
	    
	    System.exit(cleanUpToolRunner);
	    
	}

	//Method to delete the given directory
	protected void DirectoryDelete(String path) throws IOException{
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path dPath = new Path(path);
		
		if(fileSystem.exists(dPath))//check for the existence of the given path, if exists, then delete it
			fileSystem.deleteOnExit(dPath);
	}

}
