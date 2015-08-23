import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class mapreduce {
    static ForkJoinPool pool = new ForkJoinPool();

    public static <K, Key, Value> Map<Key, Value> run(List<K> ins,
            final Mapper<K,Map<Key, Value>> mapper,
            final Reducer<Value> reducer) {


        final ConcurrentHashMap<Key, ConcurrentLinkedQueue<Value>> dicts =
            new ConcurrentHashMap<>();
        List<Callable<Value>> tasks = new ArrayList<>(ins.size());
        for (final K in : ins) {
            Callable<Value> task =
                new Callable<Value> () {
                    @Override
                    public Value call() {
                        Map<Key, Value> map = mapper.map(in);
                        for (Map.Entry<Key, Value> entry : map.entrySet()) {
                            Key key = entry.getKey();
                            Value value = entry.getValue();
                            if (! dicts.containsKey(key)) {
                                ConcurrentLinkedQueue<Value> queue = new ConcurrentLinkedQueue<>();
                                dicts.putIfAbsent(key, queue);
                            }
                            dicts.get(key).add(value);
                        }
                        return null;
                    }
                };
            tasks.add(task);
        }
        pool.invokeAll(tasks);

        int n_reducers = dicts.entrySet().size();
        tasks = new ArrayList<>(n_reducers);
        final ConcurrentHashMap<Key, Value> results = new ConcurrentHashMap<>(n_reducers);
        for (final Map.Entry<Key, ConcurrentLinkedQueue<Value>> entry : dicts.entrySet()) {
            Callable<Value> task =
                new Callable<Value> () {
                    @Override
                    public Value call() {
                        results.put(entry.getKey(),
                                reducer.reduce(entry.getValue())
                                );
                        return null;
                    }
                };
            tasks.add(task);
        }
        pool.invokeAll(tasks);

        return new HashMap<>(results);
    }

    static interface Mapper<K,V> {
        V map(K key);
    }

    static interface Reducer<V> {
        V reduce(Iterable<V> vs);
    }

    static class WordCount implements Mapper<BufferedReader, Map<String, Integer>>, Reducer<Integer> {
        File input_dir;
        File output;

        public WordCount(String input_dir, String output) {
            this.input_dir = new File(input_dir);
            this.output = new File(output);
        }

        public List<BufferedReader> getInputs() {
            List<BufferedReader> inputs = new ArrayList<>();
            for (File f : input_dir.listFiles()) {
                try {
                    inputs.add(new BufferedReader(new FileReader(f)));
                } catch (Exception e) {
                    System.out.println("file read Exception");
                }
            }
            return inputs;
        }

        public void putOutput(Map<String, Integer> map) {
            BufferedWriter writer = null;
            try {
                writer = new BufferedWriter(new FileWriter(output));
            } catch (Exception e) {
                System.out.println("file write exception");
            }
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String str = "(\"" + entry.getKey() + "\"," + entry.getValue() + ")\n";
                try {
                    writer.write(str, 0, str.length());
                } catch (Exception e) {
                    System.out.println("file write exception");
                }
            }
            try {
                writer.close();
            } catch (Exception e) {
                System.out.println("IO failure");
            }
        }

        @Override
        public Map<String, Integer> map(BufferedReader file) {
            Map<String, Integer> dict = new HashMap<>();
            String line = null;
            while(true) {
                try {
                    line = file.readLine();
                    if (line == null) {
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("IO failure");
                }
                for (String word : line.split("\\s+")) {
                    word = word.replaceAll("[^\\w]", "");
                    if (word.isEmpty()) {
                        continue;
                    }
                    Integer value = dict.get(word);
                    if (value == null) {
                        dict.put(word, 1);
                    } else {
                        dict.put(word, value+1);
                    }
                }
            }
            try {
                file.close();
            } catch (Exception e) {
                System.out.println("IO failure");
            }
            return dict;
        }
        @Override
        public Integer reduce(Iterable<Integer> counts) {
            int sum = 0;
            for (int s : counts) {
                sum += s;
            }
            return sum;
        }
    }

    public static void main(String[] args) {
        WordCount wc = new WordCount("./in", "./out/output.txt");
        wc.putOutput(mapreduce.run(wc.getInputs(), wc, wc));
    }
}
