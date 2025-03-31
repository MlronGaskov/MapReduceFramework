import ru.nsu.mr.AfterMapReducer;
import ru.nsu.mr.OutputContext;

import java.util.Iterator;

public class Reducer1 extends AfterMapReducer<String, Integer, String, Integer> {
    @Override
    public void reduce(String key, Iterator<Integer> values, OutputContext<String, Integer> output) {

    }
}
