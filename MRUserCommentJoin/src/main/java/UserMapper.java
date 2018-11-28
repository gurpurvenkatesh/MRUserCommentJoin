import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<Object, Text, IntWritable, Text>{

	private IntWritable outKey = new IntWritable(0);
	private Text outValue = new Text();
			
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		String userId = parsed.get("Id");
		String userName = parsed.get("DisplayName");
		
		if(userId == null)
			return;
		
		outKey.set(Integer.parseInt(userId));			/* Setting id to be key */
		outValue.set("!" + userName);					/* Setting userName to be value */
		
		context.write(outKey, outValue);				
	}
}
