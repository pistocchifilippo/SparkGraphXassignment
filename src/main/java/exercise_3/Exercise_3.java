package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long, Path, Path, Path> implements Serializable {

        @Override
        public Path apply(Long vertexID, Path vertexValue, Path message) {
            if (message.getCost() == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
                message.addVertexToPath(vertexID);
                return new Path(Math.min(vertexValue.getCost(), message.getCost()), message.getPath());
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Path,Integer>, Iterator<Tuple2<Object, Path>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Path>> apply(EdgeTriplet<Path, Integer> triplet) {
            Tuple2<Object, Path> v_origin = triplet.toTuple()._1();
            Tuple2<Object, Path> v_destination = triplet.toTuple()._2();
            if (v_origin._2.getCost() + triplet.toTuple()._3() < v_destination._2.getCost()
                    && v_origin._2.getCost() != Integer.MAX_VALUE ) {
                // propagate source vertex value + value of the edge
                v_origin._2.setCost(v_origin._2.getCost() + triplet.toTuple()._3());
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Path>(triplet.dstId(), v_origin._2)).iterator()).asScala();
            } else {
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Path>>().iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Path, Path, Path> implements Serializable {
        @Override
        public Path apply(Path o, Path o2) {
            if (o.getCost() <= o2.getCost()) {
                return o2;
            }
            else {
                return o;
            }
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object, Path>> vertices = Lists.newArrayList(
                new Tuple2<>(1L,new Path(0, 1L)),
                new Tuple2<>(2L,new Path(Integer.MAX_VALUE, 2L)),
                new Tuple2<>(3L,new Path(Integer.MAX_VALUE, 3L)),
                new Tuple2<>(4L,new Path(Integer.MAX_VALUE, 4L)),
                new Tuple2<>(5L,new Path(Integer.MAX_VALUE, 5L)),
                new Tuple2<>(6L,new Path(Integer.MAX_VALUE, 6L))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<>(1L,2L, 4), // A --> B (4)
                new Edge<>(1L,3L, 2), // A --> C (2)
                new Edge<>(2L,3L, 5), // B --> C (5)
                new Edge<>(2L,4L, 10), // B --> D (10)
                new Edge<>(3L,5L, 3), // C --> E (3)
                new Edge<>(5L, 4L, 4), // E --> D (4)
                new Edge<>(4L, 6L, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Path>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Path,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Path(Integer.MAX_VALUE), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Path.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Path.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(
                new Path(Integer.MAX_VALUE),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Path.class))
                .vertices()
                .toJavaRDD()
                .sortBy(rdd -> ((Tuple2<Object, Path>) rdd)._1, true, 1)
                .foreach(v -> {
                    Tuple2<Object, Path> vertex = (Tuple2<Object, Path>) v;
                    String shortestPath = "[";
                    for (Long vertexOnPath : vertex._2.getPath()) {
                        shortestPath = shortestPath + labels.get(vertexOnPath) + ", ";
                    }
                    shortestPath = shortestPath.substring(0, shortestPath.length()-2) + "]";
                    System.out.println("Minimum cost to get from "+labels.get(1L)+" to "+ labels.get(vertex._1) +
                            " is " +shortestPath + " with cost " + vertex._2.getCost());
                });
    }

}