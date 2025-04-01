import ru.nsu.mr.InMapReducer;
import ru.nsu.mr.OutputContext;

import java.util.Iterator;

public class Reducer2 extends InMapReducer<String, Integer, String, Integer> {
    @Override
    public void reduce(String key, Iterator<Integer> values, OutputContext<String, Integer> output) {

    }

    @Override
    public void generalReduce(String key, Iterator<Integer> values, OutputContext<String, Integer> output) {

    }
}
