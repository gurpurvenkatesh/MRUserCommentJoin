import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UCJoinReducer extends Reducer<IntWritable, Text, Text, Text>{

	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	private String joinType = null;
	
	protected void setup(Context context) throws IOException, InterruptedException {

		joinType = context.getConfiguration().get("join.type");		/* extracting the join type from configuration property */
	}
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		listA.clear();
		listB.clear();
		
		/* One reducer will get all the values of same key(userid) 
		 * Add the values either name or comments into separate list depending on identifier */
		
		for(Text val : values) {		
			
			if(val.charAt(0) == '!'){
				listA.add(new Text(val.toString().substring(1)));
			}else if (val.charAt(0) == '@'){
				listB.add(new Text(val.toString().substring(1)));
			}
		} 
		
		performJoins(context);		/* performs join operation */
	}
	
	
	public void performJoins(Context context) throws IOException, InterruptedException {
	
		if(joinType.equalsIgnoreCase("inner")){				/* Checks if its inner join */
			if(!listA.isEmpty() && !listB.isEmpty()){		/* if both the lists are not empty */
				for(Text A : listA){						/* iterate through every entry in List A for users */
					for (Text B : listB){					/* Gets the corresponding value from List B(comments) for given userid */
						context.write(A, B);				/* Final output key value pair */
					}
				}
			}
		}else{
			throw new RuntimeException("Join Type not set");
		}
	
	}

}
