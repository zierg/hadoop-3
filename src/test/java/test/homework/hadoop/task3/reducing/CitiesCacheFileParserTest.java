package test.homework.hadoop.task3.reducing;

import com.google.common.collect.ImmutableMap;
import homework.hadoop.task3.reducing.CitiesCacheFileParser;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CitiesCacheFileParserTest {

    @Test
    public void test() throws URISyntaxException {
        //noinspection ConstantConditions
        URI uri = getClass().getClassLoader().getResource("cities-test-file.txt").toURI();
        val parser = new CitiesCacheFileParser(uri);
        Map<String, String> cities = parser.getCities();
        assertThat(cities)
                .hasSize(EXPECTED.size())
                .containsAllEntriesOf(EXPECTED);
    }

    Map<String, String> EXPECTED = ImmutableMap.<String, String>builder()
            .put("0", "unknown")
            .put("1", "moscow")
            .put("2", "tolyatti")
            .build();
}