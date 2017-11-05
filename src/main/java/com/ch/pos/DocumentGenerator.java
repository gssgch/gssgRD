package main.java.com.ch.pos;

import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ch on 2017/6/7.
 * Email: 824203453@qq.com
 */
public class DocumentGenerator {
    private Configuration configuration = null;

    public static void main(String[] args) throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("date", "2012");
        map.put("modifyDate", "2011/8/29");
        map.put("modifyUser", "Zhu You Feng");
        new DocumentGenerator().createDoc("", "D:/outFile2.doc", map);
    }

    public DocumentGenerator() {
        configuration = new Configuration();
        configuration.setDefaultEncoding("utf-8");
        configuration.setClassicCompatible(true);
        configuration.setTemplateLoader(new ByteArrayStreamTemplateLoader(new ByteArrayInputStream(
                getBytesFromFile(new File("D:/ownProject/freemarkerToDoc/src/main/resources/docTemplate/doc1.ftl"))
        )));
    }

    /**
     * @param fileName
     * @param outFileName
     * @param dataMap
     */
    public void createDoc(String fileName, String outFileName, Map dataMap) {
        Template t = null;
        try {
            t = configuration.getTemplate(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        File outFile = new File(outFileName);
        Writer out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        try {
            t.process(dataMap, out);
        } catch (TemplateException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] getBytesFromFile(File f) {
        if (f == null) {
            return null;
        }
        try {
            FileInputStream stream = new FileInputStream(f);
            ByteArrayOutputStream out = new ByteArrayOutputStream(1000);
            byte[] b = new byte[1000];
            int n;
            while ((n = stream.read(b)) != -1)
                out.write(b, 0, n);
            stream.close();
            out.close();
            return out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}


class ByteArrayStreamTemplateLoader implements TemplateLoader {

    InputStream in = null;

    public ByteArrayStreamTemplateLoader(ByteArrayInputStream inputStream) {
        in = inputStream;
    }

    public Object findTemplateSource(String name) throws IOException {
        System.out.println("findTemplateSource");
        return in;
    }

    public long getLastModified(Object templateSource) {
        return 0;
    }

    public Reader getReader(Object templateSource, String encoding) throws IOException {
        System.out.println("getReader");
        return new InputStreamReader(in);
    }

    public void closeTemplateSource(Object templateSource) throws IOException {
        System.out.println("closeTemplateSource");
        if (in != null) {
            in.close();
        }
    }
}