# S3 Java Demo

Set up your environment for Java with Visual Studio with e.g. https://blog.usejournal.com/visual-studio-code-for-java-the-ultimate-guide-2019-8de7d2b59902

Java AWS SDK Guide
https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/welcome.html

Maven Config
https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-project-maven.html

cd s3

Maven Assembly for jar with dependencies
mvn assembly:assembly -DdescriptorId=jar-with-dependencies

Run jar
java -jar target/

https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html