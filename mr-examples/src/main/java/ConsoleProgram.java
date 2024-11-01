import ru.nsu.mr.Mapper;
import ru.nsu.mr.OutputContext;
import ru.nsu.mr.Pair;
import ru.nsu.mr.Reducer;

import java.util.Iterator;

public class ConsoleProgram {
    static class WordCountMapper implements Mapper<String, String, String, Integer> {
        @Override
        public void map(
                Iterator<Pair<String, String>> input, OutputContext<String, Integer> output) {
            while (input.hasNext()) {
                Pair<String, String> split = input.next();
                for (String word : split.value().split("[\\s.,]+")) {
                    output.put(word.trim().toLowerCase(), 1);
                }
            }
        }
    }

    static class WordCountReducer implements Reducer<String, Integer, String, Integer> {
        @Override
        public void reduce(
                String key, Iterator<Integer> values, OutputContext<String, Integer> output) {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next();
            }
            output.put(key, sum);
        }
    }

    public static void main(String[] args) {
        System.out.println("Hello world!");
    }
}
