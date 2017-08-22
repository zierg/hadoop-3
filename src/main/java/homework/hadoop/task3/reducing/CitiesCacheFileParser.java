package homework.hadoop.task3.reducing;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CitiesCacheFileParser {

    public CitiesCacheFileParser(URI uri, JobContext context) {
        file = new org.apache.hadoop.fs.Path(uri.getPath());
        this.context = context;
    }

    public Map<String, String> getCities() {
        cities = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(file.toString()))))) {
            String line;
            while ((line = reader.readLine()) != null) {
                parseLine(line);
            }
        } catch (IOException ex) {
            log.error("Error during reading the cache file", ex);
            throw new RuntimeException(ex);
        }
        return cities;
    }

    private void parseLine(String line) {
        if (line.matches(LINE_REGEX)) {
            String[] split = line.split("\\s");
            cities.put(split[0], split[1]);
        } else {
            log.warn("Incorrect line in the cities file: {}", line);
        }
    }

    Path file;
    JobContext context;

    @NonFinal
    Map<String, String> cities;

    static String LINE_REGEX = "[^\\s]+\\s[^\\s]+";

    static Logger log = LoggerFactory.getLogger(CitiesCacheFileParser.class);
}
