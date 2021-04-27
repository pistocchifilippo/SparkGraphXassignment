package exercise_3;

import scala.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Path implements Serializable{

    private Integer cost;
    private List<Long> path;

    public Path(Integer cost)
    {
        this.path = new ArrayList<>();
        this.cost = cost;
    }

    public Path(Integer cost, List<Long> path)
    {
        this(cost);
        this.path.addAll(path);
    }

    public Path(Integer cost, Long... path)
    {
        this(cost, Arrays.asList(path));
    }

    public Integer getCost() {
        return cost;
    }

    public void setCost(Integer cost) {
        this.cost = cost;
    }

    public List<Long> getPath() {
        return path;
    }

    public void setPath(List<Long> path) {
        this.path = path;
    }

    public void addVertexToPath(Long id) {
        path.add(id);
    }

}