package com.join.pc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * @author neshant
 *
 */
public class PostCommentBuildingDriver {


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: PostCommentHierarchy <posts> <comments> <outdir>");
			System.exit(1);
		}

		
		Path outputDir = new Path(args[2]);
		Job job = new Job(conf, "PostCommentHierarchy");
		job.setJarByClass(PostCommentBuildingDriver.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, PostMapper.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, CommentMapper.class);

		job.setReducerClass(PostCommentHierarchyReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}

	
}