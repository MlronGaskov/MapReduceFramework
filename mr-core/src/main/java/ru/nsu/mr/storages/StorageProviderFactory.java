package ru.nsu.mr.storages;

public class StorageProviderFactory {
    public static StorageProvider getStorageProvider(String connectionString) {
        if (connectionString == null || connectionString.trim().isEmpty()
                || connectionString.trim().equalsIgnoreCase("local")) {
            return new LocalStorageProvider();
        } else if (connectionString.startsWith("S3:")) {
            String s3ConnectionString = connectionString.substring(3).trim();
            return new S3StorageProvider(s3ConnectionString);
        } else if (connectionString.startsWith("MINIO:")) {
            String minioConnectionString = connectionString.substring(6).trim();
            try {
                return new MinIOStorageProvider(minioConnectionString);
            } catch (Exception e) {
                throw new RuntimeException("Error initializing MinIOStorageProvider", e);
            }
        } else {
            throw new IllegalArgumentException("Invalid connection string: " + connectionString);
        }
    }
}
