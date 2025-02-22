package ru.nsu.mr.storages;

import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;

public class S3StorageProvider implements StorageProvider {

    private final S3Client s3Client;
    private final String bucketName;

    public S3StorageProvider(String connectionString) {
        Map<String, String> configMap = parseConfig(connectionString);
        this.bucketName = configMap.get("bucketName");
        this.s3Client = connectS3Client(configMap);
    }

    private S3Client connectS3Client(Map<String, String> configMap) {
        System.out.println(configMap);
        return S3Client.builder()
                .region(Region.of(configMap.get("region")))
                .endpointOverride(URI.create(configMap.get("endpoint")))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                configMap.get("accessKey"),
                                configMap.get("secretKey")
                        )
                ))
                .build();
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
        Path parent = destination.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.deleteIfExists(destination);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(destination));
    }

    @Override
    public void put(Path source, String key) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        s3Client.putObject(putObjectRequest, RequestBody.fromFile(source));
    }

    @Override
    public List<String> list(String key) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(key)
                .build();
        ListObjectsV2Response response = s3Client.listObjectsV2(request);
        return response.contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
        if (s3Client != null) {
            s3Client.close();
        }
    }
}
