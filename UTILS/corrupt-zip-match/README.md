# Corrupt Zip Match 
## Identify which matched filename is corrupt and threw an IOException

We can use wildcards to read multiple files matching a glob pattern. When the number of files scales up it can be difficult to identify if a single one is corrupt and causes the Dataflow job to fail.

## Quickstart

You can use the provided `run.sh` script as in:
```bash
./run.sh <PROJECT_ID> <BUCKET_NAME>
```

Alternatively, follow these steps:
* Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way 
* Set the `$PROJECT` and `$BUCKET` variables
* Create data, gzip files and corrupt one of them:

```bash
echo "id,message" >> input_ok.csv
echo "1,hello" >> input_ok.csv
echo "id,message" >> input_bad.csv
echo "2,world" >> input_bad.csv

gzip input_ok.csv 
gzip input_bad.csv 

truncate -s-6 input_bad.csv.gz
```

* Verify corrupt `.gz` file:

```bash
$ gunzip input_bad.csv.gz

gzip: input_bad.csv.gz: unexpected end of file
```

* Upload to GCS:

```bash
gsutil cp *.csv.gz gs://$BUCKET/corrupted-file-test/
```

* Run with `Directrunner`:

```bash
mvn compile -e exec:java \
 -Dexec.mainClass=com.dataflow.samples.CorruptZip \
      -Dexec.args="--project=$PROJECT \
      --bucket=$BUCKET \
      --runner=DirectRunner"
```

This code was tested with Java SDK 2.16.0.

## Example

The code will look for files matching `gs://$BUCKET/corrupted-file-test/*"`:

```java
.apply("Get file names",Create.of("gs://" + Bucket + "/corrupted-file-test/*")) 
.apply(FileIO.matchAll())
.apply(FileIO.readMatches())
```

and then, in our parsing function, we can get the filename from a `ReadableFile` using `getMetadata().resourceId()`. There we can use a try-catch block to detect if a file throws the `IOException`:

```java
.apply(MapElements.via(new SimpleFunction <ReadableFile, String>() {
  public String apply(ReadableFile f) {
    String fileName = f.getMetadata().resourceId().toString();
    String temp = "";
    try{
      temp = f.readFullyAsUTF8String();
      Log.info(String.format("Successfully read file: %s", fileName));
    } catch(IOException e){
      Log.error(String.format("ERROR when reading file: %s", fileName));
    }

    return temp;
  }
}));
```

Output is the follwing in our case:

```bash
INFO: Matched 2 files for pattern gs://BUCKET_NAME/corrupted-file-test/*
Dec 10, 2019 9:04:52 PM com.dataflow.samples.CorruptZip$1 apply
INFO: Successfully read file: gs://BUCKET_NAME/corrupted-file-test/input_ok.csv.gz
Dec 10, 2019 9:04:52 PM com.dataflow.samples.CorruptZip$1 apply
SEVERE: ERROR when reading file: gs://BUCKET_NAME/corrupted-file-test/input_bad.csv.gz
```

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
