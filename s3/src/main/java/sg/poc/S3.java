package sg.poc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import com.amazonaws.HttpMethod;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.util.StringUtils;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * This class demonstrates different methods to make basic requests to an S3
 * endpoint
 * <p>
 * <b>Prerequisites:</b> You must have valid S3 credentials.
 * <p>
 * <b>Important:</b> Be sure to fill in your S3 access credentials in
 * ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users)
 * before you run this app.
 */
public final class S3 {

    // define some values for megabyte and kilobyte
    private static final long KB = 1 << 10;
    private static final long MB = KB << 10;
    private static final long GB = MB << 10;

    // setup logging
    private final static Logger logger = Logger.getLogger(S3.class);

    /*
     * CLI Parameters
     */
    @Parameter(names = { "--endpoint", "-e" }, description = "Custom S3 Endpoint (e.g. https://s3.example.org")
    private String endpoint = "";
    @Parameter(names = { "--size", "-s" }, description = "Size in MB")
    private int sizeInMb = 128;
    @Parameter(names = { "--insecure", "-i" }, description = "Disable SSL Certificate checking")
    private boolean insecure = false;
    @Parameter(names = { "--keep-files", "-k" }, description = "Keep upload source and download destination files")
    private boolean keepFiles = false;
    @Parameter(names = { "--profile" }, description = "AWS Profile to be used")
    private String profileName = "";
    @Parameter(names = { "--region", "-r" }, description = "AWS Region to be used")
    private String region = "";
    @Parameter(names = { "--skip-validation", "-sv" }, description = "Skip MD5 validation of uploads and downloads")
    private boolean skipValidation = false;
    @Parameter(names = { "--debug", "-d" }, description = "Enable debug level logging")
    private boolean debug = false;
    @Parameter(names = { "--help", "-h" }, help = true)
    private boolean help = false;
    @Parameter(names = { "--tempFileDirectory",
            "-t" }, description = "Path to directory were temp file should be stored")
    private String tempFileDirectory;

    // internal variables
    private Path tempFileDirectoryPath;
    private AmazonS3 s3Client;
    private TransferManager transferManager;
    private String bucketName;
    private File file;
    private long partSize;

    /**
     * S3 Demo
     * 
     * @param args The arguments of the program.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // Parameter parsing
        S3 s3 = new S3();
        JCommander jCommander = new JCommander(s3);
        jCommander.parse(args);
        if (s3.help) {
            jCommander.usage();
            return;
        }

        s3.initialize();
        s3.run();
        s3.cleanup();
    }

    private void initialize() throws IOException {
        if (debug) {
            logger.getLogger("sg.poc.S3").setLevel(Level.DEBUG);
            logger.getLogger("com.amazonaws.request").setLevel(Level.DEBUG);
            logger.getLogger("org.apache.http").setLevel(Level.DEBUG);
            logger.debug("Log level set to DEBUG");
        }

        if (endpoint.startsWith("s3-")) {
            String endpointRegion = endpoint.split("[-.]")[1];
            if (endpointRegion != region) {
                logger.warn("Endpoint URL starts with s3-" + endpointRegion + " but region " + endpointRegion
                        + " in URL does not match configured region " + region
                        + " which can result in access failures.");
            }
        }

        if (insecure) {
            logger.info("Disabling SSL certificate check, not recommended for production!");
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        }

        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        if (!profileName.isEmpty()) {
            logger.info("Setting up AWS SDK S3 Client using AWS Profile " + profileName);
            s3ClientBuilder.withCredentials(new ProfileCredentialsProvider(profileName));
        }
        if (!endpoint.isEmpty()) {
            if (region.isEmpty()) {
                region = s3ClientBuilder.getRegion();
            }
            logger.info("Setting up AWS SDK S3 Client using endpoint " + endpoint);
            s3ClientBuilder.withEndpointConfiguration(new EndpointConfiguration(endpoint, region));
        }
        s3Client = s3ClientBuilder.build();

        // not required, but showing what is possible when using your own executor
        // factory
        ExecutorFactory executorFactory = new ExecutorFactory() {
            public ExecutorService newExecutor() {
                ThreadFactory threadFactory = new ThreadFactory() {
                    private int threadCount = 1;

                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("s3-transfer-manager-worker-" + this.threadCount++);
                        return thread;
                    }
                };
                int numberOfProcessors = Runtime.getRuntime().availableProcessors();
                return Executors.newFixedThreadPool(numberOfProcessors, threadFactory);
            }
        };

        // create a Transfer Manager for managing S3 uploads and downloads with the
        // High-Level API
        TransferManagerBuilder transferManagerBuilder = TransferManagerBuilder.standard();
        transferManagerBuilder.withExecutorFactory(executorFactory).withS3Client(s3Client);
        transferManagerBuilder.withMultipartCopyThreshold(512 * MB);
        transferManagerBuilder.withMinimumUploadPartSize(128 * MB);
        transferManager = transferManagerBuilder.build();

        if (!StringUtils.isNullOrEmpty(tempFileDirectory)) {
            tempFileDirectoryPath = Paths.get(tempFileDirectory);
        } else {
            tempFileDirectoryPath = Files.createTempDirectory("s3-performance-test-");
        }

        logger.info("Directory to store temporary files: " + tempFileDirectoryPath);

        bucketName = UUID.randomUUID().toString();
        logger.info("Using random bucket name" + bucketName);

        file = Files.createTempFile(tempFileDirectoryPath, "s3-", ".dat").toFile();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(sizeInMb * MB);
        randomAccessFile.close();
        logger.info("Created temporary file " + file + " of size " + sizeInMb + "MB");
    }

    private void run() throws IOException {
        logger.info("Creating bucket " + bucketName);
        CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName);
        s3Client.createBucket(createBucketRequest);

        logger.info("Listing all buckets");
        List<Bucket> buckets = s3Client.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getName());
        }

        logger.info("Write object with content-type text/plain and content \"Hello World!\"");
        s3Client.putObject(bucketName, "my-object", "Hello World!");

        logger.info("Copy an existing object");
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucketName, "my-object", bucketName,
                "my-object-copy");
        s3Client.copyObject(copyObjectRequest);

        logger.info("Upload object from file");
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, "my-object", file);
        s3Client.putObject(putObjectRequest);

        logger.info("Upload object from file using transferManager");
        transferManager.upload(putObjectRequest);

        logger.info("Upload all files of directory using transferManager and prefix object keys with my-directory/");
        putObjectRequest = new PutObjectRequest(bucketName, "my-object", file);
        transferManager.uploadDirectory(bucketName, "my-directory", tempFileDirectoryPath.toFile(), true);

        logger.info("Read object");
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, "my-object");
        File destinationFile = Files.createTempFile(tempFileDirectoryPath, "s3-", ".dat").toFile();
        s3Client.getObject(getObjectRequest, destinationFile);

        logger.info("Download file using transfer manager");
        getObjectRequest = new GetObjectRequest(bucketName, "my-object");
        transferManager.download(getObjectRequest, destinationFile);

        logger.info("Listing objects");
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName);
        // withMaxKeys is usually not required (default is 1000), just used to
        // demonstrate trunking
        listObjectsV2Request.withMaxKeys(1);
        ListObjectsV2Result listObjectsV2Result = s3Client.listObjectsV2(listObjectsV2Request);
        List<S3ObjectSummary> objects = listObjectsV2Result.getObjectSummaries();
        logger.info("Retrieved " + listObjectsV2Result.getKeyCount() + " objects");
        while (listObjectsV2Result.isTruncated()) {
            logger.info("Result is truncated, therefore we need to fetch additional objects");
            listObjectsV2Request.withContinuationToken(listObjectsV2Result.getNextContinuationToken());
            listObjectsV2Result = s3Client.listObjectsV2(listObjectsV2Request);
            objects.addAll(listObjectsV2Result.getObjectSummaries());
        }
        objects.forEach((object) -> System.out.println(object.getKey()));

        logger.info("Write object with custom metadata");
        putObjectRequest = new PutObjectRequest(bucketName, "my-object", file);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("company", "NetApp");
        metadata.addUserMetadata("location", "Europe");
        putObjectRequest.withMetadata(metadata);
        s3Client.putObject(putObjectRequest);

        logger.info("Get object metadata");
        ObjectMetadata remoteMetadata = s3Client.getObjectMetadata(bucketName, "my-object");
        remoteMetadata.getRawMetadata().forEach((key, value) -> System.out.println(key + " => " + value.toString()));

        logger.info("Add tag to object");
        List<Tag> tags = new ArrayList<Tag>();
        tags.add(new Tag("projectId", "12345"));
        tags.add(new Tag("company", "NetApp"));
        ObjectTagging objectTagging = new ObjectTagging(tags);
        SetObjectTaggingRequest setObjectTaggingRequest = new SetObjectTaggingRequest(bucketName, "my-object",
                objectTagging);
        s3Client.setObjectTagging(setObjectTaggingRequest);

        logger.info("Get object tags");
        GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(bucketName, "my-object");
        GetObjectTaggingResult getObjectTaggingResult = s3Client.getObjectTagging(getObjectTaggingRequest);
        getObjectTaggingResult.getTagSet().forEach((tag) -> System.out.println(tag.getKey() + " => " + tag.getValue()));

        logger.info("Creating presigned URL");
        GeneratePresignedUrlRequest generatePresignedUrl = new GeneratePresignedUrlRequest(bucketName, "my-object",
                HttpMethod.GET);
        Date expiration = new Date(System.currentTimeMillis() - 3600 * 1000);
        generatePresignedUrl.withExpiration(expiration);
        URL presignedUrl = s3Client.generatePresignedUrl(generatePresignedUrl);
        logger.info("Presigned URL: " + presignedUrl);

        logger.info("Delete individual object");
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, "my-object");
        s3Client.deleteObject(deleteObjectRequest);

        logger.info("Delete all objects in bucket " + bucketName
                + " using Multi-Object Delete using previously retrieved object list");
        while (objects.size() > 0) {
            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName);
            List<S3ObjectSummary> objectsToDelete = objects.subList(0, Math.min(objects.size(), 1000));
            List<KeyVersion> objectKeysToDelete = objectsToDelete.stream()
                    .map(objectSummary -> new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()))
                    .collect(Collectors.toList());
            deleteObjectsRequest.withKeys(objectKeysToDelete);
            objects.removeAll(objectsToDelete);
            s3Client.deleteObjects(deleteObjectsRequest);
        }

        // logger.info("Setting public read only bucket policy");
        // String policyPublicReadOnly = "{ \"Statement\": [ { \"Effect\": \"Allow\",
        // \"Principal\": \"*\", \"Action\": [ \"s3:GetObject\", \"s3:ListBucket\" ],
        // \"Resource\": [ \"urn:sgws:s3:::website-bucket\",
        // \"urn:sgws:s3:::website-bucket/*\" ] } ] }";
        // SetBucketPolicyRequest setBucketPolicyRequest = new
        // SetBucketPolicyRequest(bucketName, policyPublicReadOnly);
        // s3Client.setBucketPolicy(setBucketPolicyRequest);

        DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
        logger.info("Delete bucket " + bucketName);
        s3Client.deleteBucket(deleteBucketRequest);

        logger.info("Finished");
    }

    private void cleanup() throws IOException {
        logger.info("Deleting temp file directory " + tempFileDirectoryPath);
        Files.walkFileTree(tempFileDirectoryPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes basicFileAttributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });

        // shutdown Transfer Manager to release threads
        transferManager.shutdownNow();

        // shutdown S3 Client
        s3Client.shutdown();
    }
}
