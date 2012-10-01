package sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 * <code>
 * sample arguments: src/test/resources/sample/rfc2616.txt src/test/resources/sample/rfc2616.txt
 * </code>
 * 
 * @author benjamin
 */
public class WordCountTest {

    public static void main(String[] args) throws IOException {
        long before = System.currentTimeMillis();
        Map<String, Integer> counts = new TreeMap<String, Integer>();

        File file = new File(args[0]);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                    String next = tokenizer.nextToken();
                    Integer count = counts.get(next);
                    if (count == null)
                        counts.put(next, 1);
                    else
                        counts.put(next, count + 1);
                }
            }
        } finally {
            reader.close();
        }

        File output = new File(args[1]);
        Writer writer = new FileWriter(output);
        try {
            for (Entry<String, Integer> entry : counts.entrySet())
                writer.write(entry.getKey() + "\t" + entry.getValue() + "\n");
        } finally {
            writer.close();
        }
        System.out.println("Done: "+(System.currentTimeMillis() - before));
    }
}
