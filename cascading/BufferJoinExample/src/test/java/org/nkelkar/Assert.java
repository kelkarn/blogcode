package org.nkelkar;

import java.io.File;

import org.fest.assertions.Assertions;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * Shortcut to compare the content of two files.
 */
public class Assert {

    public static void sameContent(String actualOutputPath, String expectedOutputPath) throws Exception {
        String actualContent = contentOf(actualOutputPath);
        String expectedContent = contentOf(expectedOutputPath);
        Assertions.assertThat(actualContent).isEqualTo(expectedContent);

    }

    private static String contentOf(String filePath) throws Exception {
        return Files.toString(new File(filePath), Charsets.UTF_8);
    }

}

