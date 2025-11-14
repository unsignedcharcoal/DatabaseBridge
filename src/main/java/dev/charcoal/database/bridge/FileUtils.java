package dev.charcoal.database.bridge;


import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.net.URL;

public class FileUtils {

    public static File getFileFromResource(String resourcePath) {
        URL resourceUrl = FileUtils.class.getClassLoader().getResource(resourcePath);

        if (resourceUrl == null) {
            throw new IllegalArgumentException("Resource not found: " + resourcePath);
        }

        return new File(resourceUrl.getFile());
    }


    public static @NotNull File getOrCreateFile(String folder, String fileName){
        File file = new File(folder + File.separator + fileName);
        if (!file.exists()) {
            if (file.mkdirs()) {
                try {
                    if (file.createNewFile()){
                        return file;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }

        return file;
    }

}
