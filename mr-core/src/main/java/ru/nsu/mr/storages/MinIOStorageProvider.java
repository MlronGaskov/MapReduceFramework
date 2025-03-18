package ru.nsu.mr.storages;

import io.minio.MinioClient;
import io.minio.DownloadObjectArgs;
import io.minio.UploadObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.BucketExistsArgs;
import io.minio.Result;
import io.minio.messages.Item;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

public class MinIOStorageProvider implements StorageProvider {
    private final MinioClient minioClient;
    private final String bucketName;

    public MinIOStorageProvider(String connectionString) throws IOException {
        Map<String, String> configMap = parseConfig(connectionString);
        this.bucketName = configMap.get("bucketName");
        try {
            this.minioClient = MinioClient.builder()
                    .endpoint(configMap.get("endpoint"))
                    .credentials(configMap.get("accessKey"), configMap.get("secretKey"))
                    .build();

            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }
        } catch (Exception e) {
            throw new IOException("Error initializing MinIO client", e);
        }
    }

    private Map<String, String> parseConfig(String config) {
        Map<String, String> configMap = new HashMap<>();
        String[] pairs = config.split(";");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length == 2) {
                configMap.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }
        return configMap;
    }

    @Override
    public void get(String key, Path destination) throws IOException {
        try {
            if (destination.getParent() != null) {
                Files.createDirectories(destination.getParent());
            }
            minioClient.downloadObject(
                    DownloadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(key)
                            .filename(destination.toString())
                            .build()
            );
        } catch (Exception e) {
            throw new IOException("Error downloading object from MinIO", e);
        }
    }

    @Override
    public void put(Path source, String key) throws IOException {
        try {
            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(key)
                            .filename(source.toString())
                            .build()
            );
        } catch (Exception e) {
            throw new IOException("Error uploading object to MinIO", e);
        }
    }

    @Override
    public List<String> list(String key) throws IOException {
        List<String> objects = new ArrayList<>();
        try {
            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(bucketName)
                            .prefix(key)
                            .recursive(true)
                            .build()
            );
            for (Result<Item> result : results) {
                objects.add(result.get().objectName());
            }
        } catch (Exception e) {
            throw new IOException("Error listing objects in MinIO", e);
        }
        return objects;
    }

    @Override
    public void close() throws Exception {

    }
}
