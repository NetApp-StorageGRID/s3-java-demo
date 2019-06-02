package sg.poc;

import org.apache.commons.codec.binary.Hex;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.util.IOUtils;
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

    // set number of processors
    private final static int numberOfProcessors = Runtime.getRuntime().availableProcessors();

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
    @Parameter(names = { "--region", "-r" }, description = "AWS Region to be used")
    private String region = "us-east-1";
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
            logger.info("Disabling certificate check");
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        }

        if (!endpoint.isEmpty()) {
            logger.info("Setting up AWS SDK S3 Client using endpoint " + endpoint + " and region " + region);
            s3Client = AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
                    .withEndpointConfiguration(new EndpointConfiguration(endpoint, region)).build();
        } else {
            logger.info("Setting up AWS SDK S3 Client using AWS endpoint and region ");
            s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
        }

        // S3 allows max part size of 5GB, calculating partSize so that every processor
        // core gets 1 thread
        if (sizeInMb / (long) numberOfProcessors > 5 * GB) {
            partSize = 5 * GB;
        } else if (sizeInMb / numberOfProcessors < 5 * MB) {
            partSize = 5 * MB;
        } else {
            partSize = sizeInMb * MB / numberOfProcessors;
        }

        // create a new executor factory to enable multi-threaded uploads with one
        // thread per processor
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
                return Executors.newFixedThreadPool(numberOfProcessors, threadFactory);
            }
        };

        // create a Transfer Manager for managing S3 uploads and downloads with the AWS
        // SDK High-Level API
        logger.info("Number of processors: " + numberOfProcessors);
        transferManager = TransferManagerBuilder.standard().withExecutorFactory(executorFactory).withS3Client(s3Client)
                .withMinimumUploadPartSize(partSize).withMultipartUploadThreshold(partSize).build();
        logger.info("Part size: " + partSize);

        if (!StringUtils.isNullOrEmpty(tempFileDirectory)) {
            tempFileDirectoryPath = Paths.get(tempFileDirectory);
        } else {
            tempFileDirectoryPath = Files.createTempDirectory("s3-performance-test-");
        }

        logger.info("Directory to store temporary files: " + tempFileDirectoryPath);

        bucketName = UUID.randomUUID().toString();
        logger.info("Using random bucket name" + bucketName);

        logger.info("Creating sample file of size " + sizeInMb + "MB");
        file = createFile(sizeInMb, tempFileDirectoryPath);

    }

    private void run() throws IOException {
        logger.info("Creating bucket " + bucketName);
        s3Client.createBucket(bucketName);

        logger.info("Listing all buckets");
        List<Bucket> buckets = s3Client.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getName());
        }

        logger.info("Write object with content-type text/plain and content \"Hello World!\"");
        s3Client.putObject(bucketName, "my-object", "Hello World!");

        logger.info("Copy an existing object");
        s3Client.copyObject(bucketName, "my-object", bucketName, "my-object-copy");

        logger.info("Upload object from file");
        s3Client.putObject(bucketName, "my-object", file);

        logger.info("Write object using transferManager");
        transferManager.upload(bucketName, "my-object", file);

        logger.info("Create 10 empty objects with prefix folder/");
        for (int i = 1; i <= 10; i++) {
            s3Client.putObject(bucketName, "folder/" + i, "");
        }

        logger.info("Read object");
        S3Object downloadObject = s3Client.getObject(bucketName, "my-object");
        S3ObjectInputStream inputStream = downloadObject.getObjectContent();

        File destinationFile = createFile(sizeInMb,tempFileDirectoryPath);
        logger.info("Path to destination file: " + destinationFile.getAbsolutePath());

        // copy object to file and calculate MD5 sum while copying
        try {
            // create temporary File to save download to
            OutputStream outputStream = new FileOutputStream(destinationFile);
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            DigestInputStream digestInputStream = new DigestInputStream(inputStream, messageDigest);
            IOUtils.copy(digestInputStream, outputStream);
            String md5sum = Hex.encodeHexString(messageDigest.digest());
            logger.info("MD5 sum of destination file: " + md5sum);
            inputStream.close();
            outputStream.close();
        } catch (java.security.NoSuchAlgorithmException noSuchAlgorithmException) {
            noSuchAlgorithmException.printStackTrace();
        }
        finally {
            // delete destination file
            if (!keepFiles) {
                destinationFile.delete();
            }
        }

        logger.info("Download file using transfer manager");
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, "my-object");
        transferManager.download(getObjectRequest, destinationFile);

        logger.info("Listing objects");
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request().withMaxKeys(5).withBucketName(bucketName);
        ListObjectsV2Result listObjectsV2Result = s3Client.listObjectsV2(listObjectsV2Request);
        int keyCount = listObjectsV2Result.getKeyCount();
        List<S3ObjectSummary> objects = listObjectsV2Result.getObjectSummaries();
        logger.info("Retrieved " + keyCount + " objects");
        while (listObjectsV2Result.isTruncated()) {
            logger.info("result is truncated, therefore we need to fetch additional objects");
            listObjectsV2Request.withContinuationToken(listObjectsV2Result.getNextContinuationToken());
            listObjectsV2Result = s3Client.listObjectsV2(listObjectsV2Request);
            objects.addAll(listObjectsV2Result.getObjectSummaries());
        }
        objects.forEach((object) -> System.out.println(object.getKey()));

        logger.info("Write object with custom metadata");
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, "my-object", file);
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
        getObjectTaggingResult.getTagSet().forEach((tag) -> System.out.println(tag.getKey() + " => " +  tag.getValue()));

        logger.info("Delete all objects in bucket " + bucketName + " using Multi-Object Delete");
        while (objects.size() > 0) {
            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName);
            List<S3ObjectSummary> objectsToDelete = objects.subList(0, Math.min(objects.size(), 1000));
            ;
            List<KeyVersion> objectKeysToDelete = objectsToDelete.stream()
                    .map(objectSummary -> new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()))
                    .collect(Collectors.toList());
            deleteObjectsRequest.withKeys(objectKeysToDelete);
            objects.removeAll(objectsToDelete);
            s3Client.deleteObjects(deleteObjectsRequest);
        }

        logger.info("Delete bucket " + bucketName);
        s3Client.deleteBucket(bucketName);

        logger.info("Finished");
    }

    private void cleanup() {
        logger.info("Deleting temp file directory " + tempFileDirectoryPath);
        /* Files.walkFileTree(tempFileDirectoryPath, new SimpleFileVisitor<Path>() {
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
        }); */

        // shutdown Transfer Manager to release threads
        transferManager.shutdownNow();
    }

    /**
     * Creates a sample file of size sizeInMb. If tempFileDirectory is not empty the
     * file will be created in tempFileDirectory, otherwise in the default temp
     * directory of the OS.
     *
     * @param sizeInMb              File size
     * @param tempFileDirectoryPath Optional directory to be used for storing
     *                              temporary file
     * @return A newly created temporary file of size sizeInMb
     * @throws IOException
     */
    private File createFile(final long sizeInMb, final Path tempFileDirectoryPath) throws IOException {
        logger.info("Creating temporary file");
        Path tempFile = Files.createTempFile(tempFileDirectoryPath, "s3-sample-file-", ".dat");
        logger.info("Created temporary file " + tempFile);
        RandomAccessFile randomAccessFile = new RandomAccessFile(tempFile.toFile(), "rw");
        randomAccessFile.setLength(sizeInMb * MB);
        randomAccessFile.close();

        return tempFile.toFile();
    }
}
