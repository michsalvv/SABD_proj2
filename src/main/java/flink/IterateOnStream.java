package flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateOnStream {

    /*
     * We create a topology as follows:
     *
     *  someIntegers -(iteration)-> minusOne -> <> ->
     *                    ^_____________________|
     *
     * Si itera per un certo numero di volte sui dati in stream, fino al verificarsi di una certa condizione
     */
    // NOTA: nell'output abbiamo alcuni errori perché stiamo eseguendo su IDE locale, sul cluster non dovrebbero esserci
    // Output:
//        6> 1  (X)
//        7> 2  (o)
//        3> -1 (X)
//        4> 0  (X)
//        5> 2  (o)
//        2> 2  (o)
//        1> 2  (o)
//        8> 2  (o)
//        9> 2  (o)
//        10> 2 (o)
//        11> 2 (o)
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        long threshold = 2L;

        DataStream<Long> someIntegers = env.fromSequence(0, 10);
//        someIntegers.print();
//        System.out.println("______________________________-");
        IterativeStream<Long> iteration = someIntegers.iterate();
//        iteration.print();

        DataStream<Long> minusOne = iteration.map(new Decrement());
        DataStream<Long> stillGreaterThanZero = minusOne.filter(new GreaterThan(threshold));
        iteration.closeWith(stillGreaterThanZero); // Specifico che l'iterazione si deve chiudere in questo punto

        DataStream<Long> result = minusOne.filter(new SmallerEqualThan(threshold));
        result.print();

        env.execute("Iterate on Stream");
    }

    public static class Decrement implements MapFunction<Long, Long> {
        @Override
        public Long map(Long value) throws Exception {
            return value - 1 ;
        }
    }

    public static class GreaterThan implements FilterFunction<Long> {
        private Long threshold;
        GreaterThan(Long threshold){
            this.threshold = threshold;
        }
        @Override
        public boolean filter(Long value) throws Exception {
            return (value > threshold);
        }
    }
    public static class SmallerEqualThan implements FilterFunction<Long> {
        private Long threshold;
        SmallerEqualThan(Long threshold){
            this.threshold = threshold;
        }
        @Override
        public boolean filter(Long value) throws Exception {
            return (value <= threshold);
        }
    }
}
