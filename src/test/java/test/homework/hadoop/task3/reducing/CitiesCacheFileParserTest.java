package test.homework.hadoop.task3.reducing;

import com.google.common.collect.ImmutableMap;
import homework.hadoop.task3.reducing.CitiesCacheFileParser;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CitiesCacheFileParserTest {

    @Test
    public void test() throws URISyntaxException {
        JobContext context = getJobContext();
        //noinspection ConstantConditions
        URI uri = getClass().getClassLoader().getResource(TEST_FILE).toURI();
        val parser = new CitiesCacheFileParser(uri, context);
        Map<String, String> cities = parser.getCities();
        assertThat(cities)
                .hasSize(EXPECTED.size())
                .containsAllEntriesOf(EXPECTED);
    }

    private JobContext getJobContext() {
        JobContext context = mock(JobContext.class);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        when(context.getConfiguration()).thenReturn(conf);
        return context;
    }

    Map<String, String> EXPECTED = ImmutableMap.<String, String>builder()
            .put("0", "unknown")
            .put("1", "moscow")
            .put("2", "tolyatti")
            .build();

    static String TEST_FILE = "cities-test-file.txt";
}