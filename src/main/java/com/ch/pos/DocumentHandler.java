package main.java.com.ch.pos;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * Created by ch on 2017/6/7.
 * Email: 824203453@qq.com
 */
public class DocumentHandler {
    private Configuration configuration = null;

    public DocumentHandler() {
        configuration = new Configuration();
        configuration.setDefaultEncoding("utf-8");
    }

    public static void main(String[] args) {
        new DocumentHandler().createDoc();
    }

    public void createDoc() {
        // 要填入模本的数据文件
        Map dataMap = new HashMap();
        getData(dataMap);
        // 设置模本装置方法和路径,FreeMarker支持多种模板装载方法。可以重servlet，classpath，数据库装载，
        // 这里我们的模板是放在com.havenliu.document.template包下面
        System.out.println(this.getClass());
        configuration.setClassForTemplateLoading(this.getClass(), "/main/java/com/ch/pos");
        Template t = null;
        try {
            // test.ftl为要装载的模板
            t = configuration.getTemplate("test-xx_details.ftl");
            t.setEncoding("utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 输出文档路径及名称
        File outFile = new File("D:/test.doc");
        Writer out = null;
        try {
            FileOutputStream fos = new FileOutputStream(outFile);
            OutputStreamWriter oWriter = new OutputStreamWriter(fos, "UTF-8");
            //这个地方对流的编码不可或缺，使用main（）单独调用时，应该可以，但是如果是web请求导出时导出后word文档就会打不开，并且包XML文件错误。主要是编码格式不正确，无法解析。
            out = new BufferedWriter(oWriter);
            /** out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), "utf-8")); **/
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        try {
            t.process(dataMap, out);
            out.close();
        } catch (TemplateException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 注意dataMap里存放的数据Key值要与模板中的参数相对应
     *
     * @param dataMap
     */
    private void getData(Map dataMap) {
        dataMap.put("title_name", "用户信息");
        dataMap.put("user_name", "张三");
        dataMap.put("org_name", "微软公司");
        dataMap.put("dept_name", "事业部");
    }
}