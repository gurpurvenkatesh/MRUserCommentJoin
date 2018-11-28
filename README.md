# MRUserCommentJoin
MapReduce Applicatio which needed to process 2 data sets using 2 mappers. And the mapper output will be processed by one reducer. 

1.	Problem Statement 
    a.	Find: Name of the user who commented on the site & when 
    b.	Data sets: Input data stored in multiple sets 
            i.	Users: Stores user info like user id, name etc. 
            ii.	Comments: Stores comment info along with the user id.  
    c.	Approach: Get the solution with both data sets using userId. 
 
2.	General Structure 
    a.	Users & Comments data set is processed separately through 2 Mappers. 
    b.	In the Mapper retain only those columns which are required for the join operation & the resultant output. 
    c.	Mapper output will have UserId as the key & the data required for the join operations as value. 
    d.	Since we have 2 mappers the outputs needed to be properly flagged through any identifier so that Reducer function can easily identify & understand the data input.  
    e.	Map output is sent to the Partitioner & the Reducer performs the Join & Reduce operation. 

3.	Detailed Design 
    a.	User.xml & Comments.xml is stored as XML file format :  
        i.	Users dataset has the value in id attribute & the comments dataset has the value in the userId attribute. 
        ii.	Need DisplayName from users dataset 
        iii.	Need CreationDate from Comments dataset.

    b.	Mapper Logic
        i.	User input : <byteOffset, XML record> : <IntWritable, Text> 
        ii.	Comments input : <byteOffset, XML record> : <IntWritable, Text> 
        iii.	Parse XML and extract userid & DisplayName from User dataset and id & CreationDate from Comments dataset. 
        iv.	Prefix the value with ! Or @ 
        v.	Output the {key, value} pair 

    c.	Map Output & Reducer input : Since we have 2 mappers the outputs needed to be properly flagged through any identifier so that Reducer function can easily identify & understand the data input. So the output would like something like below. 
        i.	User output : <userId, !userName> : <IntWritable, Text> 
        ii.	Comments output : <userId, @CreatedDate> : <IntWritable, Text> 

    d.	Reducer Logic
        i.	All the MapOutput for a specific userid will be sent to only one reducer 
        ii.	Reducer need to get the information for this userid from both User & comment dataset. 
        iii.	For the same userId it has to get the UserName & Comment Creation Date. 
        iv.	Get the type of join that’s being performed : This value is passes by user through command line 
        v.	Iterate through the each value of same key from the multiple Map output and seggragate them ito different output depending on whether they are from user's dataset or comments dataset. So 2 lists are used. 
        vi.	Check the first character of the Map Output value pair & devide the output into 2 separate lists. 
        vii.	Based on the type of iterate through the lists to find matching records & output them into HDFS. 

    e.	Driver class 
        i.	Need to specify 2 input paths & one output paths in the command line. 
        ii.	Also need to mention the Type of Join through the command line. 
        iii.	User multiple input class which basically supports the mapreduce job with different input format & mapper for each of these input paths. 
        iv.	In our case both the input format are xml format. But in case if one of the file is different like Json, then such configuration changes can be captured in the MapReduce driver program
