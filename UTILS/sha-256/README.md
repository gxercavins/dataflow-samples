## Calculate SHA-256 Hash per file

The main idea, stemmed from this [StackOverflow question](https://stackoverflow.com/questions/55784467/how-do-i-get-a-readable-file) is to calculate the hash of a batch of files matching a pattern.

## Quickstart

Set up [authentication](https://cloud.google.com/docs/authentication/) your preferred way.

You can run the code in `run.sh` passing two arguments: `INPUT` and `OUTPUT`. Main code is found in `DataflowSHA256.java`.

This code was tested with the Direct Runner and 2.11.0 version of the Java SDK.

## Example

`FileIO.readMatches()` will return a PCollection of `ReadableFile`. For each one, we can read the filename with `getMetadata().resourceId()` and `readFullyAsUTF8String()` to get the full file content as described [here](https://beam.apache.org/releases/javadoc/2.11.0/org/apache/beam/sdk/io/FileIO.html). We can use a try/catch block to make it resilient against unwanted matches (i.e. hidden files and so). Notice that here we are reading the file at once and not line per line as we want to calculate the hash. For that, we can use the `DigestUtils` library with `DigestUtils.sha256Hex(file.readFullyAsUTF8String())`:

```java
p
  .apply("Match Filenames", FileIO.match().filepattern(options.getInput()))
  .apply("Read Matches", FileIO.readMatches())
  .apply(MapElements.via(new SimpleFunction <ReadableFile, KV<String,String>>() {
      public KV<String,String> apply(ReadableFile f) {
            String temp = null;
            try{
                temp = f.readFullyAsUTF8String();
            }catch(IOException e){

            }

            String sha256hex = org.apache.commons.codec.digest.DigestUtils.sha256Hex(temp);   
            
            return KV.of(f.getMetadata().resourceId().toString(), sha256hex);
        }
      }
  ))
```

The output in my case (dummy files in `data` folder) was:

```java
Apr 21, 2019 10:02:21 PM com.dataflow.samples.DataflowSHA256$2 processElement
INFO: File: /home/.../data/file1, SHA-256: e27cf439835d04081d6cd21f90ce7b784c9ed0336d1aa90c70c8bb476cd41157 
Apr 21, 2019 10:02:21 PM com.dataflow.samples.DataflowSHA256$2 processElement
INFO: File: /home/.../data/file2, SHA-256: 72113bf9fc03be3d0117e6acee24e3d840fa96295474594ec8ecb7bbcb5ed024
```

Which we can verify with an online hashing [tool][1]:

[![enter image description here][2]][2]

  [1]: https://md5file.com/calculator
  [2]: https://i.stack.imgur.com/0j7l3.png

## License

These examples are provided under the Apache License 2.0.

## Issues

Report any issue to the GitHub issue tracker.
