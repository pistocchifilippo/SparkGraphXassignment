package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Exercise_4 {

	// constants
	final static String FILE_SEPARATOR = File.separator;
	final static String RESOURCES_FILE_PATH =
			"src" + FILE_SEPARATOR +
			"main" + FILE_SEPARATOR +
			"resources";
	final static String EDGES_FILE_PATH = RESOURCES_FILE_PATH + FILE_SEPARATOR + "wiki-edges.txt";
	final static String VERTICES_FILE_PATH = RESOURCES_FILE_PATH + FILE_SEPARATOR + "wiki-vertices.txt";

	// loading procedure and Page Rank
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

		final List<Row> vertices_list =
				new ResourcesReaderImpl().getResource(VERTICES_FILE_PATH, new Splitter());

		final List<Row> edges_list =
				new ResourcesReaderImpl().getResource(EDGES_FILE_PATH, new Splitter());

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("article", DataTypes.StringType, true, new MetadataBuilder().build()),
		});
		
		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
		});

		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);
		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		GraphFrame gf = GraphFrame.apply(vertices,edges);

		System.out.println(gf);

		gf.edges().show();
		gf.vertices().show();

	}

}





interface ResourcesReader {
	List<Row> getResource(final String filePath, final ResourcesParser parser);
}

interface ResourcesParser {
	Row parse(final String line);
}

class Splitter implements ResourcesParser {

	final static String SEPARATOR = "\t";

	@Override
	public Row parse(String line) {
		final String[] splits = line.split(SEPARATOR);
		return RowFactory.create(splits[0],splits[1]);
	}
}

class ResourcesReaderImpl implements ResourcesReader {

	@Override
	public List<Row> getResource(String filePath, final ResourcesParser parser) {

		final List<Row> tuples = new LinkedList<>();

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


