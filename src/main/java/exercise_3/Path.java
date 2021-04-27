package exercise_3;

import com.clearspring.analytics.util.Lists;
import scala.Serializable;

import java.util.ArrayList;
import java.util.Arrays;

public class Path implements Serializable {
    private Integer cost;
    private ArrayList<String> currentPath;
    private String s;

    public Path(Integer cost) {
        this.cost = cost;
        this.currentPath = new ArrayList<String>();
        System.out.println("DONE");
    }

    public Path(Integer cost, String s) {
        this.cost = cost;
        ArrayList temp = new ArrayList<String>();
        temp.add(s);
        this.currentPath = temp;

        System.out.println("EEEEE");
    }

    public Path(Integer cost, ArrayList<String> currentPath) {
        this.cost = cost;
        this.currentPath = currentPath;
    }

    public Integer getCost() {
        return cost;
    }

    public ArrayList<String> getCurrentPath() {
        return currentPath;
    }

    public void setCost(Integer cost) {
        this.cost = cost;
    }

    public void setCurrentPath(ArrayList<String> currentPath) {
        this.currentPath = currentPath;
    }

}
