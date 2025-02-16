package ru.nsu.mr.storages;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StorageProviderTest {

    private static final String LOCAL_TEST_DIR = "test_local_storage";
    private static StorageProvider localStorage;
    private static StorageProvider s3Storage;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void setUp() {
        localStorage = new LocalStorageProvider();

        String s3Endpoint = System.getenv("endpoint");
        String accessKey = System.getenv("accessKey");
        String secretKey = System.getenv("secretKey");
        String bucketName = System.getenv("bucketName");
        String region = System.getenv("region");

        if (s3Endpoint == null || accessKey == null || secretKey == null || bucketName == null || region == null) {
            throw new IllegalStateException("S3 environment variables are not set correctly");
        }

        String s3ConnectionString = String.format("endpoint=%s;accessKey=%s;secretKey=%s;bucketName=%s;region=%s",
                s3Endpoint, accessKey, secretKey, bucketName, region);

        s3Storage = new S3StorageProvider(s3ConnectionString);
    }

    @Test
    @Order(1)
    void testLocalStorageUploadAndDownload() throws Exception {
        Path sourceFile = tempDir.resolve("test.txt");
        Files.writeString(sourceFile, "Hello, world!");

        String key = LOCAL_TEST_DIR + "/test.txt";
        localStorage.upload(sourceFile, key);

        Path destinationFile = tempDir.resolve("downloaded.txt");
        localStorage.download(key, destinationFile);

        assertTrue(Files.exists(destinationFile));
        assertEquals("Hello, world!", Files.readString(destinationFile));
    }

    @Test
    @Order(2)
    void testLocalStorageList() {
        List<String> files = localStorage.list(LOCAL_TEST_DIR);

        assertFalse(files.isEmpty());
        assertTrue(files.contains(LOCAL_TEST_DIR + "/test.txt"));
    }

    @Test
    @Order(3)
    void testS3UploadAndDownload() throws Exception {
        Path sourceFile = tempDir.resolve("s3_test.txt");
        Files.writeString(sourceFile, "S3 test file");

        String key = "s3_test.txt";
        s3Storage.upload(sourceFile, key);

        Path destinationFile = tempDir.resolve("s3_downloaded.txt");
        s3Storage.download(key, destinationFile);

        assertTrue(Files.exists(destinationFile));
        assertEquals("S3 test file", Files.readString(destinationFile));
    }

    @Test
    @Order(4)
    void testS3List() {
        String key = "";
        List<String> files = s3Storage.list(key);

        assertFalse(files.isEmpty());
        assertTrue(files.contains("s3_test.txt"));
    }


    @Test
    @Order(5)
    void testLocalStorageUploadToNestedDirectoryAndDownload() throws Exception {
        Path sourceFile = tempDir.resolve("nested_test.txt");
        Files.writeString(sourceFile, "Content in nested directory");

        String key = LOCAL_TEST_DIR + "/level1/level2/level3/nested_test.txt";
        localStorage.upload(sourceFile, key);

        Path destinationFile = tempDir.resolve("nested_downloaded.txt");
        localStorage.download(key, destinationFile);

        assertTrue(Files.exists(destinationFile));
        assertEquals("Content in nested directory", Files.readString(destinationFile));
    }

    @Test
    @Order(6)
    void testS3UploadToNestedDirectoryAndDownload() throws Exception {
        Path sourceFile = tempDir.resolve("s3_nested_test.txt");
        Files.writeString(sourceFile, "S3 content in nested directory");

        String key = "nested/level1/level2/s3_nested_test.txt";
        s3Storage.upload(sourceFile, key);

        Path destinationFile = tempDir.resolve("s3_nested_downloaded.txt");
        s3Storage.download(key, destinationFile);

        assertTrue(Files.exists(destinationFile));
        assertEquals("S3 content in nested directory", Files.readString(destinationFile));
    }

    @Test
    @Order(7)
    void testLocalStorageOverwrite() throws Exception {
        Path sourceFile = tempDir.resolve("overwrite_test.txt");
        Files.writeString(sourceFile, "Original content");
        String key = LOCAL_TEST_DIR + "/overwrite_test.txt";
        localStorage.upload(sourceFile, key);

        Path destinationFile = tempDir.resolve("overwrite_downloaded.txt");
        localStorage.download(key, destinationFile);
        assertTrue(Files.exists(destinationFile));
        assertEquals("Original content", Files.readString(destinationFile));

        Files.writeString(sourceFile, "Updated content");
        localStorage.upload(sourceFile, key);

        localStorage.download(key, destinationFile);
        assertEquals("Updated content", Files.readString(destinationFile));
    }

    @Test
    @Order(8)
    void testS3Overwrite() throws Exception {
        Path sourceFile = tempDir.resolve("s3_overwrite_test.txt");
        Files.writeString(sourceFile, "S3 Original content");
        String key = "s3_overwrite_test.txt";
        s3Storage.upload(sourceFile, key);

        Path destinationFile = tempDir.resolve("s3_overwrite_downloaded.txt");
        s3Storage.download(key, destinationFile);
        assertTrue(Files.exists(destinationFile));
        assertEquals("S3 Original content", Files.readString(destinationFile));

        Files.writeString(sourceFile, "S3 Updated content");
        s3Storage.upload(sourceFile, key);

        s3Storage.download(key, destinationFile);
        assertEquals("S3 Updated content", Files.readString(destinationFile));
    }

    @AfterAll
    static void tearDown() throws Exception {
        localStorage.close();
        s3Storage.close();
    }
}
