package utils;

import scala.Serializable;
import utils.tuples.ValQ3;

import java.util.ArrayList;
import java.util.List;

public class Grid implements Serializable {
    private Vertex top_left;
    private Vertex bottom_right;
    private Vertex top_right;
    private Vertex bottom_left;
    private List<Cell> cells;
    int split_factor;


    public Grid(Double lat1, Double lon1, Double lat2, Double lon2, int split_factor) {
        this.bottom_left = new Vertex(lat1, lon1);
        this.top_right = new Vertex(lat2, lon2);
        this.bottom_right = new Vertex(lat1, lon2);
        this.top_left = new Vertex(lat2, lon1);
        this.split_factor = split_factor;
        this.split();
    }

    //[cell_id, top_left={latitude1, longitude1}, bottom_right={latitude2, longitude2}]
    //lat1 -> lat2: verso su, lon1 -> lon2: verso destra
    public void split() {
        this.cells = new ArrayList<>(split_factor);
        Double offset_lat = ((top_left.getLat()- bottom_left.getLat())/split_factor);
//        System.out.println("lat_per_area: " + offset_lat);
        Double offset_lon = ((top_right.getLon()- top_left.getLon())/split_factor);
//        System.out.println("lon_per_area: " + offset_lon);
        Integer cell_id = 0;
        for (int i = 0; i < split_factor; i++) {
            Double cell_lat1 = top_left.getLat()-(i*offset_lat);
            for (int j = 0; j < split_factor; j++) {
                Double cell_lon1 = top_left.getLon()+(j*offset_lon);
                Double cell_lat2 = cell_lat1-offset_lat;
                Double cell_lon2 = cell_lon1+offset_lon;
                Cell c = new Cell(cell_id,
                         new Vertex(cell_lat1, cell_lon1),
                         new Vertex(cell_lat2, cell_lon2));
                cells.add(c);
                cell_id++;
            }
        }
    }

    public Vertex getTop_left() {
        return top_left;
    }

    public void setTop_left(Vertex top_left) {
        this.top_left = top_left;
    }

    public Vertex getBottom_right() {
        return bottom_right;
    }

    public void setBottom_right(Vertex bottom_right) {
        this.bottom_right = bottom_right;
    }

    public int getSplit_factor() {
        return split_factor;
    }

    public void setSplit_factor(int split_factor) {
        this.split_factor = split_factor;
    }

    public List<Cell> getCells() {
        return cells;
    }

    public void setCells(List<Cell> cells) {
        this.cells = cells;
    }
}

    public class Vertex implements Serializable {
        Double lat;
        Double lon;

        public Vertex(Double lat, Double lon) {
            this.lat = lat;
            this.lon = lon;
        }
        public Double getLat() {
            return lat;
        }
        public void setLat(Double lat) {
            this.lat = lat;
        }
        public Double getLon() {
            return lon;
        }
        public void setLon(Double lon) {
            this.lon = lon;
        }
    }

    public static class Cell implements Serializable {
        Integer id;
        Vertex top_left;
        Vertex bottom_right;

        public Cell(Integer id, Vertex topLeft, Vertex bottomRight) {
            this.id = id;
            this.top_left = topLeft;
            this.bottom_right = bottomRight;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public Vertex getTop_left() {
            return top_left;
        }

        public void setTop_left(Vertex top_left) {
            this.top_left = top_left;
        }

        public Vertex getBottom_right() {
            return bottom_right;
        }

        public void setBottom_right(Vertex bottom_right) {
            this.bottom_right = bottom_right;
        }
    }
}
