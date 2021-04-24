package cn.king.producer;

import cn.king.entry.avro.generated.User;
import com.csvreader.CsvReader;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

/**
 * @author: wjl@king.cn
 * @time: 2021-04-24 17:52
 * @version: 1.0.0
 * @description:
 * @why:
 */
@Slf4j
public class UserCsvFileReader implements Supplier<User> {

    private final String filePath;
    private final String regex;
    private CsvReader csvReader;

    public UserCsvFileReader(String filePath, String regex) {
        this.filePath = filePath;
        this.regex = regex;
        try {
            csvReader = new CsvReader(filePath);
            csvReader.readHeaders();
        } catch (Exception e) {
            log.error("Error reading TaxiRecords from file: " + filePath, e);
        }
    }

    @Override
    public User get() {
        User user = new User();
        try {
            if (csvReader.readRecord()) {
                String record = csvReader.getRawRecord();
                String[] split = record.split(regex);
                if (split.length == 3) {
                    user.setId(Long.valueOf(split[0]));
                    user.setUsername(split[1]);
                    user.setAge(Integer.valueOf(split[2]));
                    return user;
                }
                return null;
            }
        } catch (Exception e) {
            log.error("Exception from " + filePath);
        }

        return null;
    }

}
