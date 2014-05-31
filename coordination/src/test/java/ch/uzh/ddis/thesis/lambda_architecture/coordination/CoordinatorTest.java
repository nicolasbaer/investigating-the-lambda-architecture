package ch.uzh.ddis.thesis.lambda_architecture.coordination;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class CoordinatorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testGetFilesFromPath() throws Exception {
        Set<String> fileNames = new HashSet<>();
        File testDir = folder.newFolder();
        for(int i = 0; i < 10; i++){
            File newFile = new File(testDir.getPath() + System.getProperty("file.separator") + "test" + i + ".csv");
            newFile.createNewFile();
            fileNames.add(newFile.getName());
        }

        String path = CoordinatorTest.class.getResource("/file_pattern_test").getPath();

        Coordinator coordinator = new Coordinator();
        coordinator.path = testDir.getPath();

        Collection<File> files = coordinator.getFilesFromPath();

        Assert.assertTrue(!files.isEmpty());
        for(File file : files){
            Assert.assertTrue(fileNames.contains(file.getName()));
        }

    }
}