package javac.classloader.test;

import java.net.URL;

/**
 * ProjectName:    gxb_resource
 * Package:        javac.classloader.test
 * ClassName:      testClassLoaders
 * Description:     类作用描述
 * Author:          作者：龙飞
 * CreateDate:     2020/4/14 18:21
 * Version:        1.0
 */

public class testClassLoaders {
    public static void main(String[] args) {
//Class


        URL[] url = sun.misc.Launcher.getBootstrapClassPath().getURLs();
        for(java.net.URL u: url){

            System.out.println(u.toExternalForm());
        }
        System.out.println("sa");
    }
}
