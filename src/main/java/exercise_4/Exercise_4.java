package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import scala.Tuple2;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

	}

	public static void main(String[] args) throws IOException {
		final String FILE_SEPARATOR = File.separator;
		final String RESOURCES_FILE_PATH =
				"src" + FILE_SEPARATOR +
				"main" + FILE_SEPARATOR +
				"resources";
		final String EDGES_FILE_PATH = RESOURCES_FILE_PATH + FILE_SEPARATOR + "wiki-edges.txt";
		final String VERTICES_FILE_PATH = RESOURCES_FILE_PATH + FILE_SEPARATOR + "wiki-vertices.txt";


		final List<Tuple2<String,String>> edges =
				new EdgesReader().getResource(EDGES_FILE_PATH, s -> {
					final String[] splits = s.split("\t");
					return new Tuple2(splits[0],splits[1]);
				});

	}

}


interface ResourcesReader {
	List<Tuple2<String,String>> getResource(final String filePath, final ResourcesParser parser);
}

interface ResourcesParser {
	Tuple2<String,String> parse(final String line);
}

class EdgesReader implements ResourcesReader {

	@Override
	public List<Tuple2<String, String>> getResource(String filePath, final ResourcesParser parser) {

		final List<Tuple2<String, String>> tuples = new LinkedList<>();
		try {
			final BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line;
			while ((line = reader.readLine()) != null) {
				tuples.add(parser.parse(line));
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return tuples;
	}

}
