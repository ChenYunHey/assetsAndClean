//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.lakesoul.assets.util;

public class AssetsUtils {
    public AssetsUtils() {
    }

    public static String[] parseFileOpsString(String fileOPs) {
        String[] fileInfo = new String[]{fileOPs.split(",")[1], fileOPs.split(",")[2]};
        return fileInfo;
    }
}
