package homework.hadoop.task3;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public final class OsTypeDivider {

    public int getNumberForOs(String os) {
        return "Windows".equalsIgnoreCase(os) ? 0 : 1;
    }

    private OsTypeDivider() {}
}
