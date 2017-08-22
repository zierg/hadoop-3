package homework.hadoop.task3.reducing;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CitiesCacheFileParser {

    public CitiesCacheFileParser(URI uri) {
        file = Paths.get(uri);
    }

    public Map<String, String> getCities() {
        cities = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(file)) {
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

    @NonFinal
    Map<String, String> cities;

    Path file;

    static String LINE_REGEX = "[^\\s]+\\s[^\\s]+";

    static Logger log = LoggerFactory.getLogger(CitiesCacheFileParser.class);
}
