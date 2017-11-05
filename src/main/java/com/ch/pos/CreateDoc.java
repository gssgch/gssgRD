package main.java.com.ch.pos;
import freemarker.template.Configuration;
import freemarker.template.Template;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ch on 2017/6/7.
 * Email: 824203453@qq.com
 */
public class CreateDoc {

    private Configuration configuration = null;

    public CreateDoc() {
        configuration = new Configuration();
        configuration.setDefaultEncoding("utf-8");
    }

    public void create() throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("year", "2011");
        map.put("detailsPrintDate", "2011/8/29");
        map.put("modifyUser", "Zhu You Feng");
        map.put("ushop", "xxx2");
        map.put("amount", "xxx3");


        configuration.setClassForTemplateLoading(this.getClass(), "/main/java/com/ch/pos");
        Template t = configuration.getTemplate("test-xx22.ftl","utf-8");
        File outFile = new File("D:/outFile.doc");
        Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)));
        t.process(map, out);
    }

    public static void main(String[] args) throws Exception {
        new CreateDoc().create();

    }
}
