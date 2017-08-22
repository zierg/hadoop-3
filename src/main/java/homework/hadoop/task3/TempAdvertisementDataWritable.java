package homework.hadoop.task3;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TempAdvertisementDataWritable implements WritableComparable<TempAdvertisementDataWritable> {

    Text cityId;
    Text osType;

    public TempAdvertisementDataWritable() {
        cityId = new Text();
        osType = new Text();
    }

    public TempAdvertisementDataWritable setCityId(String cityId) {
        this.cityId.set(cityId);
        return this;
    }

    public TempAdvertisementDataWritable setOsType(String osType) {
        this.osType.set(osType);
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        cityId.write(out);
        osType.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityId.readFields(in);
        osType.readFields(in);
    }

    @Override
    public int compareTo(@Nonnull TempAdvertisementDataWritable o) {
        int cities = cityId.compareTo(o.getCityId());
        return cities != 0 ? cities : osType.compareTo(o.getOsType());
    }
}
