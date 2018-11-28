import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentsMapper extends Mapper<Object, Text, IntWritable, Text>{

	private IntWritable outKey = new IntWritable(0);
	private Text outValue = new Text();
			
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		String userId = parsed.get("UserId");
		String commentCreationDate = parsed.get("CreationDate");
		
		if(userId == null)
			return;
		
		outKey.set(Integer.parseInt(userId));			/* Setting id to be key */
		outValue.set("@" + commentCreationDate);		/* Setting Comment CreationDate to be value */
		
		context.write(outKey, outValue);				
	}	
	
}
