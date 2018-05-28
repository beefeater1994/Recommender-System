import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value = movieB\tmovieA=relativeRelation
			//outputKey = movieB
			//outputValue = movieA=relativeRelation
			String[] movie_relation = value.toString().trim().split("\t");
			context.write(new Text(movie_relation[0]), new Text(movie_relation[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value = user,movie,rating
			String[] user_movie_rating = value.toString().trim().split(",");
			context.write(new Text(user_movie_rating[1]), new Text(user_movie_rating[0] + ":" + user_movie_rating[2]));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//inputKey = movieB
			//inputValue = <movieA1=relation, movieA2=relation... user1:rating1, user2;rating2...>
			//Multiplication
			//movie_relation_map<movieA_id, relativeRelation> movieA -> A1,A2,A3
			//user_rating_map<userId, rating> user -> 1,2
			//outputKey = movieA:userId
			//outputValue = relativeRelation*rating

			Map<String, Double> movie_relation_map = new HashMap<String, Double>();
			Map<String, Double> user_rating_map = new HashMap<String, Double>();
			for (Text value : values) {
				if (value.toString().contains("=")) {
					String[] movie_relation = value.toString().trim().split("=");
					movie_relation_map.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
				} else {
					String[] user_rating = value.toString().trim().split(":");
					user_rating_map.put(user_rating[0], Double.parseDouble(user_rating[1]));
				}
			}

			for (Map.Entry<String, Double> entry : movie_relation_map.entrySet()) {
				String movieA = entry.getKey();
				double relation = entry.getValue();
				for (Map.Entry<String, Double> element : user_rating_map.entrySet()) {
					String userId = element.getKey();
					double rating = element.getValue();
					String outputKey = userId + ":" + movieA;
					double outputValue = relation * rating;
					context.write(new Text(outputKey), new DoubleWritable(outputValue));
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
