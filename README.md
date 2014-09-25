gcs-buddy
=========

An easy-to-use java client for the Google Cloud Storage (GCS) API.

### Features

* automatic retry on qualifying API errors
* simplified object and prefix iteration
* upload/download progress monitoring
* ability to read objects directly using Guava's [ByteSource](http://docs.guava-libraries.googlecode.com/git-history/release/javadoc/com/google/common/io/ByteSource.html) and [CharSource](http://docs.guava-libraries.googlecode.com/git-history/release/javadoc/com/google/common/io/CharSource.html)
* convenience functions for identifying "implicit directories", composing objects, and more
