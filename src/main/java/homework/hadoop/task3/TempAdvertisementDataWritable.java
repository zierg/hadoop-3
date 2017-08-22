package homework.hadoop.task3;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TempAdvertisementDataWritable implements Writable {

    IntWritable amount;
    IntWritable osTypeGroupNumber;

    public TempAdvertisementDataWritable() {
        amount = new IntWritable();
        osTypeGroupNumber = new IntWritable();
    }

    public TempAdvertisementDataWritable setAmount(int amount) {
        this.amount.set(amount);
        return this;
    }

    public TempAdvertisementDataWritable setOsTypeGroupNumber(int osTypeGroupNumber) {
        this.osTypeGroupNumber.set(osTypeGroupNumber);
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        amount.write(out);
        osTypeGroupNumber.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        amount.readFields(in);
        osTypeGroupNumber.readFields(in);
    }
}
