package queries.flink;

import flink.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import queries.Query;
import scala.Tuple3;
import utils.Config;
import utils.Grid;
import utils.Tools;
import utils.ValQ3;

import java.util.List;

public class Query3 extends Query {
    StreamExecutionEnvironment env;
    DataStreamSource<Event> src;

    public Query3(StreamExecutionEnvironment env, DataStreamSource<Event> src) {
        this.env = env;
        this.src = src;
    }

    @Override
    public void execute() throws Exception {
        String outputPath = "q3-res";
        Double lat1 = 38.0;
        Double lon1 = 2.0;
        Double lat2 = 58.0;
        Double lon2 = 30.0;

        // construct 4x4 grid defined by the two points: (lat, lon) = (38°,2°) - (58°,30°)
        // this means that top-left = (58°,2°) and bottom_right = (38°,30°)
        Grid grid = new Grid(lat1, lon1, lat2, lon2, Config.SPLIT_FACTOR);
        List<Tuple3<Integer, Grid.Vertex, Grid.Vertex>> cells = grid.split();
        for(int i = 0; i < Config.NUM_AREAS; i++) {
            System.out.println("Cell_ID: " + cells.get(i)._1());
            System.out.println("Top_left: (" + cells.get(i)._2().getLat() + " " + cells.get(i)._2().getLon() + ")");
            System.out.println("Bottom_right: (" + cells.get(i)._3().getLat() + " " + cells.get(i)._3().getLon() + ")");;
        }

        var dataStream = src
                .map((MapFunction<Event, ValQ3>) event -> {
                    for (int i = 0; i < Config.NUM_AREAS; i++) {
                        Integer cell_id = cells.get(i)._1();
                        Grid.Vertex top_left = cells.get(i)._2();
                        Grid.Vertex bottom_right = cells.get(i)._3();
                        if (Tools.inRange(event.getLatitude(), bottom_right.getLat(), top_left.getLat())
                                && Tools.inRange(event.getLongitude(), top_left.getLon(), bottom_right.getLon())) {
                            //System.out.println("Sensor " + event.getSensor_id() + " is in cell:" + cell_id);
                            return new ValQ3(event.getTimestamp(), event.getSensor_id(), event.getLatitude(), event.getLongitude(),
                                    event.getTemperature(), event.getTemperature(), cell_id, top_left, bottom_right);
                        }
                    }
                    //discard sensor not located in the grid
                    return new ValQ3(-1);
                })
                .filter(values -> !values.getCell_id().equals(-1))
                .setParallelism(1);
        dataStream.print();

        // poi famo la keyBy, window e l'aggregate con Average per contare media
        // la mediana tocca vede come si calcola

        env.execute("Query 3");
    }
}

/**
 * Consider the latitude and longitude coordinates
 * the latitude and longitude coordinates (38 , 2 ) and
 * within the geographic area which is identified from
 * • Divide this area using a 4x4 grid and identify each
 * (58 , 30 ).
 * grid cell from the top-left to bottom-right corners using
 * the name "cell_X", where X is the cell id from 0 to 15.
 * For each cell, find the average and the median
 * temperature, taking into account the values emitted
 * from the sensors which are located inside that cell
 * -------------------------------------------------------------
 * Q3 output:
 * ts, cell_0, avg_temp0, med_temp0, ...
 * cell_15, avg_temp15, med_temp15
 */
