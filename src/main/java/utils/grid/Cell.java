package utils.grid;

import scala.Serializable;

import java.util.Objects;

public class Cell implements Serializable {
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
