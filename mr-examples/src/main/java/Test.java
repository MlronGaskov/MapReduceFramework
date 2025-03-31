import ru.nsu.mr.Reducer;

public class Test {
    public static void main(String[] args) {
        Reducer<String, Integer, String, Integer> reducer1 = new Reducer1();
        Reducer<String, Integer, String, Integer> reducer2 = new Reducer2();

        System.out.println("Reducer1 getMode(): " + reducer1.altMode()); // Ожидаем 0
        System.out.println("Reducer2 getMode(): " + reducer2.altMode()); // Ожидаем 1
    }
}
