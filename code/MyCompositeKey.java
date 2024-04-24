package code;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.*;

public class MyCompositeKey implements WritableComparable<MyCompositeKey> {
    private List<String> components;
    public MyCompositeKey(int numberOfComponents) {
        components = new ArrayList<>(numberOfComponents);
    }
    
    public MyCompositeKey() {
        components = new ArrayList<>();
    }
    public MyCompositeKey(List<String> components) {
        this.components = components;
    }
    
    public void setComponents(List<String> components) {
        this.components = components;
    }
    
    public List<String> getComponents() {
        return components;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String component : components) {
            builder.append(component).append(" ");
        }
        return builder.toString().trim();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        components.clear();
        for (int i = 0; i < size; i++){
	        components.add(in.readUTF())
        }
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt (components.size());
        for (String component : components) {
	        out.writeUTF(component);
        }
    }
    
    @Override
    public boolean compareTo(MyCompositeKey other) {
        //Compare by length first
        if (components.size() != other.components.size()) {
	        return false;
        }
        // If sizes are equal compare by components
	    for (int i = 0; i < components.size(); i++) { 
	        if(!components.get(i).equals(other.components.get(i))) {
	            return false;
	        }
	    }
	    // If all components are equal return true
        return true;
    }
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MyCompositeKey)) {
            return false;
        }
        if (this == obj) {
	        return true;
        }
        final MyCompositeKey other = (MyCompositeKey) obj;
        return components.equals(other.components);
    }
    @Override
    public int hashCode() {
        return components.hashCode();
    }
}
