package hikversion.utils;


import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileUtils {

    public static List<String> readFile(String path) throws IOException {

        List<String> sqlList = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(path));

        StringBuilder builder = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null){
            builder.append(line);
            builder.append("\n");
/*            if (line.contains(";")){
                System.out.println(line.substring(line.indexOf(";")+1) + ">>>>"+StringUtils.isBlank(line.substring(line.indexOf(";")+1)));
            }*/
            if (line.contains(";") && StringUtils.isBlank(line.substring(line.indexOf(";")+1))){
                sqlList.add(builder.substring(0,builder.lastIndexOf(";")));
                builder = new StringBuilder();
            }
        }
        reader.close();
        return sqlList;
    }

    public static Properties getProperties(String fileName) throws IOException {
        Properties p = new Properties();
        InputStream resourceAsStream = FileUtils.class.getClassLoader().getResourceAsStream(fileName);
        p.load(resourceAsStream);
        return p;
    }

}
