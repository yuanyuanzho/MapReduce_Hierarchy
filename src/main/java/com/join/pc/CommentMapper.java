package com.join.pc;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentMapper extends Mapper<Object, Text, Text, Text> {
	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// Parse the input string into a nice map
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
				.toString());

		String postId = parsed.get("PostId");
		if (postId == null) {
			return;
		}

		// The foreign join key is the user ID
		outkey.set(postId);

		// Flag this record for the reducer and then output
		outvalue.set("C" + value.toString());
		context.write(outkey, outvalue);
	}
}



