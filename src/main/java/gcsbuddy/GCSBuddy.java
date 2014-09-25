package gcsbuddy;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.googleapis.media.MediaHttpDownloaderProgressListener;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.CharSource;
import com.google.common.primitives.Longs;
import org.apache.http.NoHttpResponseException;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static gcsbuddy.Logging.info;
import static gcsbuddy.Logging.warn;

@SuppressWarnings("UnusedDeclaration")
/**
 * Provides an easy-to-use wrapper around the Google Cloud Storage (GCS) JSON API client lib, with convenience methods
 * that simplify tasks like:
 *   - iterating over lists of GCS objects
 *   - reading bytes or characters directly from an existing GCS object
 *   - composing a new GCS object in GCS out of 2 or more source objects
 *   - retrying operations on an API error
 */
public final class GCSBuddy {

  private static final String DELIMITER = "/";
  private static final CharMatcher DELIMITER_MATCHER = CharMatcher.is('/');

  // Controls how often upload/download progress is reported when using progress reporters
  private static final Long PROGRESS_INTERVAL_IN_BYTES = 1024L * 1024L;

  // Used for upload requests
  private static final String UPLOAD_CONTENT_TYPE = "application/octet-stream";
  private static final String ATTACHMENT = "attachment";

  private static final int GZIP_BUFFER_SIZE = 256 * 1024;

  // Controls the page size used when iterating over storage objects.
  private static final long DEFAULT_PAGE_SIZE = 200;

  private static final class IsRetryableGoogleException implements Predicate<Object> {
    @Override
    public boolean apply(@Nullable Object throwable) {
      checkNotNull(throwable);
      assert throwable != null;
      // GoogleJsonResponseException is a subclass of com.google.api.client.http.HttpResponseException so check it first
      if (throwable instanceof GoogleJsonResponseException) {
        final GoogleJsonError details = ((GoogleJsonResponseException) throwable).getDetails();
        return details != null && details.getCode() >= 500;
      } else if (throwable instanceof com.google.api.client.http.HttpResponseException) {
        return true;
      }
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  // operations will be retried according to the retry policy when any of the following are caught
  // during the execution of a request
  private static Predicate<Object> SHOULD_RETRY = Predicates.or(
    Lists.newArrayList(
      new IsRetryableGoogleException(),
      Predicates.instanceOf(SocketTimeoutException.class),
      Predicates.instanceOf(NoHttpResponseException.class)
    )
  );

  private static final class IsNotFound implements Predicate<Throwable> {

    @Override
    public boolean apply(Throwable input) {
      //noinspection ThrowableResultOfMethodCallIgnored
      checkNotNull(input);

      return ((GoogleJsonResponseException.class.isInstance(input)) &&
              (404 == ((GoogleJsonResponseException) input).getDetails().getCode()));
    }
  }

  private static final Predicate<Throwable> IS_NOT_FOUND = new IsNotFound();

  private static final class NAME_FN implements Function<StorageObject, String> {
    @Nullable
    @Override
    public String apply(@Nullable StorageObject object) {
      return checkNotNull(object, "object").getName();
    }
  }

  private static final class COMPOSE_SOURCE_OBJECT_FN implements Function<String, ComposeRequest.SourceObjects> {
    @Nullable
    @Override
    public ComposeRequest.SourceObjects apply(@Nullable String objectName) {
      return new ComposeRequest.SourceObjects().setName(objectName);
    }
  }

  private static String removeLeadingDelimiter(String prefix) {
    // Object prefixes look just like file paths, except they have no leading '/'
    return DELIMITER_MATCHER.trimLeadingFrom(Strings.nullToEmpty(prefix));
  }

  public static final Ordering<StorageObject> OrderedByVersion = new Ordering<StorageObject>() {
    @Override
    public int compare(@Nullable StorageObject left, @Nullable StorageObject right) {
      if (left == right) {
        return 0;
      } else if (left == null) {
        return -1;
      } else if (right == null) {
        return 1;
      } else {
        return Longs.compare(left.getGeneration(), right.getGeneration());
      }
    }
  };

  public static final Ordering<StorageObject> OrderedByModificationTime = new Ordering<StorageObject>() {
    @Override
    public int compare(@Nullable StorageObject left, @Nullable StorageObject right) {
      if (left == right) {
        return 0;
      } else if (left == null) {
        return -1;
      } else if (right == null) {
        return 1;
      } else {
        return Longs.compare(left.getUpdated().getValue(), right.getUpdated().getValue());
      }
    }
  };

  private final Provider<Storage> storage;
  private final Provider<RetryStrategy> retryStrategies;

  public GCSBuddy(final Storage storage,
                  final String bucketName) {
    this(storage, RetryStrategies.noRetry());
  }

  public GCSBuddy(final Storage storage,
                  final RetryStrategy retryStrategy) {

    this.storage = new Provider<Storage>() {
      @Override
      public Storage get() {
        return checkNotNull(storage, "storage");
      }
    };

    this.retryStrategies = new Provider<RetryStrategy>() {
      @Override
      public RetryStrategy get() {
        return checkNotNull(retryStrategy, "retryStrategy");
      }
    };
  }

  @Inject
  GCSBuddy(final Provider<Storage> storage,
           final Provider<RetryStrategy> retryStrategies) {
    this.storage = checkNotNull(storage, "storage");
    this.retryStrategies = checkNotNull(retryStrategies, "retryStrategies");
  }

  /**
   * Returns a new {@link java.util.Iterator} that pages over lists of objects with the supplied prefix
   * @param prefix the prefix to use
   * @throws IOException
   */
  private Iterator<StorageObject> newPagedObjectIterator(final String bucket, final String prefix, final boolean useVersioning) throws IOException {

    final Storage.Objects.List listRequest = storage.get()
      .objects()
      .list(bucket)
      .setMaxResults(DEFAULT_PAGE_SIZE);

    if (useVersioning) {
      listRequest.setVersions(true);
    }

    if (!Strings.isNullOrEmpty(prefix)) {
      listRequest.setPrefix(prefix);
    }

    final RetryStrategy retryStrategy = retryStrategies.get();

    return new AbstractIterator<StorageObject>() {

      private Objects currentObjects = null;
      private Iterator<StorageObject> currentObjectsIter = null;
      private boolean firstPage = true;

      /**
       * Attempts to advance to the next page of results from the list command
       * @return true if a new page was advanced to and that new page has objects, false otherwise
       * @throws IOException
       */
      private boolean advancePage() throws IOException {

        currentObjects = executeWithRetry(listRequest);

        if (currentObjects == null || currentObjects.getItems() == null) {
          return false;
        }

        currentObjectsIter = currentObjects.getItems().iterator();
        return currentObjectsIter.hasNext();
      }

      @Override
      protected StorageObject computeNext() {

        try {
          if (firstPage) {
            if (!advancePage()) {
              return endOfData();
            }
            firstPage = false;
          }

          if (currentObjectsIter.hasNext()) {
            return currentObjectsIter.next();
          } else {
            // current page is empty, get the next one
            String nextPageToken = currentObjects.getNextPageToken();
            if (nextPageToken == null) {
              return endOfData();
            }
            listRequest.setPageToken(nextPageToken);

            return advancePage() ? currentObjectsIter.next() : endOfData();
          }
        // TODO: Yuck. Since AbstractIterator's computeNext method isn't declared as throwing an IOException, we have to
        // wrap it
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }

    };
  }

  private Iterable<StorageObject> newPagedObjectIterable(final String bucket, final String prefix, final boolean useVersioning) {
    return new Iterable<StorageObject>() {
      @Override
      public Iterator<StorageObject> iterator() {
        try {
          return newPagedObjectIterator(bucket, prefix, useVersioning);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private <T> T executeWithRetry(final StorageRequest<T> request) throws IOException {

    final RetryStrategy retryStrategy = retryStrategies.get();
    Throwable cause = null;

    try {
      while(retryStrategy.retriesRemaining()) {
        try {
          return request.execute();
        } catch (Throwable e) {
          cause = e;
          if (!shouldRetry(e, retryStrategy, request)) {
            throw e;
          }
        }
      }
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    throw new RetriesExhaustedException(
      String.format("retries exhausted for request: %s using strategy: %s", request, retryStrategy), cause
    );
  }

  private InputStream executeAsInputStreamWithRetry(final StorageRequest<?> request) throws IOException {
    final RetryStrategy retryStrategy = retryStrategies.get();
	Throwable cause = null;

    try {
      while(retryStrategy.retriesRemaining()) {
        try {
          return request.executeAsInputStream();
        } catch (Throwable e) {
          cause = e;
          if (!shouldRetry(e, retryStrategy, request)) {
            throw e;
          }
        }
      }
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    throw new RetriesExhaustedException(
      String.format("retries exhausted for request: %s using strategy: %s", request, retryStrategy), cause
    );
  }

  private InputStream executeMediaAsInputStreamWithRetry(final Storage.Objects.Get request, final boolean autoDecompress)
    throws IOException {
    final RetryStrategy retryStrategy = retryStrategies.get();
    Throwable cause = null;

    try {
      while (retryStrategy.retriesRemaining()) {
        try {
          return maybeWrapInputStream(request.executeMediaAsInputStream(), request, autoDecompress);
        } catch (Throwable e) {
          cause = e;
          if (!shouldRetry(e, retryStrategy, request)) {
            throw e;
          }
        }
      }
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    throw new RetriesExhaustedException(
      String.format("retries exhausted for request: %s using strategy: %s", request, retryStrategy), cause
    );
  }

  /**
   * Blocks for some interval specified by {@code retryStrategy} iff the request should be retried (according to the
   * type of exception thrown) and returns false otherwise.
   * @param throwable the exception that was thrown while attempting to execute the request
   * @param retryStrategy the retry strategy in use
   * @param request the request being executed
   * @return true iff the request is to be retried and the sleep interval has elapsed. {@code} false if the request
   * should not be retried
   * @throws InterruptedException
   */
  private boolean shouldRetry(final Throwable throwable,
                              final RetryStrategy retryStrategy,
                              final StorageRequest<?> request) throws InterruptedException {
    if (!SHOULD_RETRY.apply(throwable)) {
      return false;
    } else {
      final long interval =  retryStrategy.nextRetryIntervalInMs();

      warn(
        "caught {0} while executing {1}; retrying in {2}ms...",
        throwable.getClass().getName(),
        request == null ? "" : request.getClass().getName(),
        interval
      );
      warn(throwable);
      Thread.sleep(interval);
      return true;
    }
  }

  /**
   * Wraps the {@link java.io.InputStream} in a {@link java.util.zip.GZIPInputStream} iff
   * the GET request was for a gzipped object, and auto decompression is enabled.
   * @param inputStream The input stream to (maybe) wrap
   * @param request The current request
   * @param autoDecompress true if auto decompression is enabled for the request, false otherwise
   * @return An {@link java.io.InputStream} instance that may or may not wrapped
   * @throws IOException
   */
  private InputStream maybeWrapInputStream(final InputStream inputStream,
                                           final Storage.Objects.Get request,
                                           final boolean autoDecompress)
    throws IOException {
    if (autoDecompress && request.getObject().endsWith(".gz")) {
      return new GZIPInputStream(inputStream, GZIP_BUFFER_SIZE);
    } else {
      return inputStream;
    }
  }

  private void verifyBucketAndPrefix(final String bucket, final String prefix) {
    checkArgument(!Strings.isNullOrEmpty(bucket), "bucket cannot be empty");
    checkNotNull(prefix, "prefix cannot be null"); // prefixes can be empty, but not null
  }

  private void verifyObjectSpecification(final String bucket, final String objectName) {
    checkArgument(!Strings.isNullOrEmpty(bucket), "bucket cannot be empty");
    checkArgument(!Strings.isNullOrEmpty(removeLeadingDelimiter(objectName)), "objectName cannot be empty");
  }

  private void verifyObjectSpecification(final String bucket, final String objectName, final long version) {
    checkArgument(!Strings.isNullOrEmpty(bucket), "bucket cannot be empty");
    checkArgument(!Strings.isNullOrEmpty(removeLeadingDelimiter(objectName)), "objectName cannot be empty");
    checkArgument(version > 0, "version must be > 0");
  }

  /**
   * Checks for the existence of an object.
   *
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to check
   * @return true if the object exists, false otherwise. This method only returns false if it receives a strict
   * "not found" response from GCS. This method will throw other codes as GoogleJsonResponseException
   * instances (such as a 403 permission error) rather than mis-identifying the specified object as non-existent.
   * @throws IOException
   */
  public boolean exists(final String bucket, final String objectName) throws IOException {
    verifyObjectSpecification(bucket, objectName);
    return exists(bucket, objectName, Optional.<Long>absent());
  }

  /**
   * Checks for the existence of an object.
   *
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to check
   * @param version the version of the object to check
   * @return true if the object exists, false otherwise. This method only returns false if it receives a strict
   * "not found" response from GCS. This method will throw other codes as GoogleJsonResponseException
   * instances (such as a 403 permission error) rather than mis-identifying the specified object as non-existent.
   * @throws IOException
   */
  public boolean exists(final String bucket, final String objectName, final long version) throws IOException {
    verifyObjectSpecification(bucket, objectName, version);
    return exists(bucket, objectName, Optional.of(version));
  }

  private boolean exists(final String bucket, final String objectName, final Optional<Long> version) throws IOException {
    try {
      final Storage.Objects.Get getRequest = storage
        .get()
        .objects()
        .get(bucket, objectName);

      if (version.isPresent()) {
        getRequest.setGeneration(version.get());
      }

      executeWithRetry(getRequest);
      return true;
    } catch (GoogleJsonResponseException e) {
      if (IS_NOT_FOUND.apply(e)) { return false; }
      throw e;
    }
  }

  /**
   * Determines if the specified object is a directory placeholder or "implicit directory".
   * @param storageObject The storage object to check
   * @return true if storageObject is a directory placeholder, false otherwise
   */
  public boolean isDirectory(StorageObject storageObject) {
    checkNotNull(storageObject, "storageObject");
    return storageObject.getName().endsWith(DELIMITER) && BigInteger.ZERO.equals(storageObject.getSize());
  }

  /**
   * Lists object names in GCS that have the given prefix.
   * @param bucket the bucket to list
   * @param prefix the prefix to use.  To list all objects in a bucket, use an empty string.
   * @return GCS object names that match the supplied prefix, if any.
   */
  public Iterable<String> list(final String bucket, final String prefix) {
    verifyBucketAndPrefix(bucket, prefix);
    return Iterables.transform(newPagedObjectIterable(bucket, removeLeadingDelimiter(prefix), false), new NAME_FN());
  }

  /**
   * Lists objects in GCS that have the given prefix.
   * @param bucket the bucket to list
   * @param prefix the prefix to use.  To list all objects in a bucket, use an empty string.
   * @return GCS {@link com.google.api.services.storage.model.StorageObject} instances that match the supplied prefix, if any.
   */
  public Iterable<StorageObject> listObjects(final String bucket, final String prefix) {
    return listObjects(bucket, prefix, false);
  }

  /**
   * Lists objects in GCS that have the given prefix.
   * @param bucket the bucket to list
   * @param prefix the prefix to use.  To list all objects in a bucket, use an empty string.
   * @param useVersioning If true, lists all versions of an object as distinct results.  If false, only the newest
   *                      (aka "live") version of an object is returned in the results.
   * @return GCS {@link com.google.api.services.storage.model.StorageObject} instances that match the supplied prefix,
   * if any.
   */
  public Iterable<StorageObject> listObjects(final String bucket, final String prefix, final boolean useVersioning) {
    verifyBucketAndPrefix(bucket, prefix);
    return newPagedObjectIterable(bucket, removeLeadingDelimiter(prefix), useVersioning);
  }

  /**
   * Lists the names of the immediate child objects of the specified bucket and prefix, treating the prefix as a
   * "directory".
   *
   * @param bucket the bucket to list
   * @param prefix the prefix to use
   */
  public Iterable<String> listChildren(final String bucket, final String prefix) {
    verifyBucketAndPrefix(bucket, prefix);

    // for matching purposes, prefixes should match what we get back from the api: no leading delimiter, and a trailing
    // delimiter (since we're treating 'prefix' as a dir)
    final String base;
    if (DELIMITER_MATCHER.lastIndexIn(prefix) != prefix.length() - 1) {
      base = DELIMITER_MATCHER.trimLeadingFrom(prefix) + DELIMITER;
    } else {
      base = DELIMITER_MATCHER.trimLeadingFrom(prefix);
    }

    final int delimiterCount = DELIMITER_MATCHER.countIn(base);

    class DelimiterCountsMatch implements Predicate<String> {
      @Override
      public boolean apply(@Nullable String result) {
        int diff = DELIMITER_MATCHER.countIn(result) - delimiterCount;
        return !base.equals(result) && (diff == 0 || (diff == 1 && DELIMITER_MATCHER.matches(result.charAt(result.length() - 1))));
      }
    }

    return FluentIterable.from(newPagedObjectIterable(bucket, removeLeadingDelimiter(base), false))
      .transform(new NAME_FN())
      .filter(new DelimiterCountsMatch());
  }

  /**
   * Lists the immediate child objects of the specified bucket and prefix, treating the prefix as a "directory".
   *
   * @param bucket the bucket to list
   * @param prefix the prefix to use
   */
  public Iterable<StorageObject> listChildObjects(final String bucket, final String prefix) {
    return listChildObjects(bucket, prefix, false);
  }

  /**
   * Lists the immediate child objects of the specified bucket and prefix, treating the prefix as a "directory".
   *
   * @param bucket the bucket to list
   * @param prefix the prefix to use
   * @param useVersioning If true, lists all versions of an object as distinct results.  If false, only the newest
   *                      (aka "live") version of an object is returned in the results.
   */
  public Iterable<StorageObject> listChildObjects(final String bucket, final String prefix, final boolean useVersioning) {
    verifyBucketAndPrefix(bucket, prefix);

    final String base;
    if ("".equals(prefix)) {
      base = "/";
    } else if (!DELIMITER_MATCHER.matches(prefix.charAt(prefix.length() - 1))) {
      base = prefix + '/';
    } else {
      base = prefix;
    }
    final int prefixCount = DELIMITER_MATCHER.countIn(base);

    class DelimitersMatch implements Predicate<StorageObject> {
      @Override
      public boolean apply(@Nullable StorageObject object) {
        String s = object.getName();
        int count = DELIMITER_MATCHER.countIn(s) - prefixCount;
        return !base.equals(s) && (count == 0 || (count == 1 && DELIMITER_MATCHER.matches(s.charAt(s.length() - 1))));
      }
    }

    return FluentIterable
      .from(newPagedObjectIterable(bucket, removeLeadingDelimiter(base), useVersioning))
      .filter(new DelimitersMatch());
  }

  /**
   * Returns the last updated timestamp of an object within the current bucket. Returns -1
   * if the object has no last updated timestamp.
   *
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to check
   * Note that this method will throw an exception if no object exists at the given path.
   *
   * @return last updated timestamp of the given object, in epoch milliseconds
   * @throws IOException
   */
  public long lastUpdatedTime(final String bucket, final String objectName) throws IOException {
    verifyObjectSpecification(bucket, objectName);
    return lastUpdatedTime(bucket, objectName, Optional.<Long>absent());
  }

  /**
   * Returns the last updated timestamp of an object within the current bucket. Returns -1
   * if the object has no last updated timestamp.
   *
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to check
   * @param version the version of the object to check
   * Note that this method will throw an exception if no object exists at the given path.
   *
   * @return last updated timestamp of the given object, in epoch milliseconds
   * @throws IOException
   */
  public long lastUpdatedTime(final String bucket, final String objectName, final long version) throws IOException {
    verifyObjectSpecification(bucket, objectName, version);
    return lastUpdatedTime(bucket, objectName, Optional.of(version));
  }

  private long lastUpdatedTime(final String bucket, final String objectName, final Optional<Long> version) throws IOException {
    final Storage.Objects.Get get = storage.get().objects().get(bucket, objectName);

    if (version.isPresent()) {
      get.setGeneration(version.get());
    }

    final StorageObject object = executeWithRetry(get);
    final com.google.api.client.util.DateTime updated = object.getUpdated();
    return (updated == null) ? -1 : updated.getValue();
  }

  /**
   * Copies a source object to a destination object
   * @param sourceBucket the bucket of the source object
   * @param sourceObjectName the name of the source object, which must already exist
   * @param destinationBucket the bucket of the destination object
   * @param destinationObjectName the name of the destination object, which may or may not already exist
   * @return a {@link com.google.api.services.storage.model.StorageObject} that represents the
   * destination object that was copied
   * @throws java.lang.RuntimeException if the source path refers to:
   *   - an object that does not exist in GCS
   *   - a prefix that is shared by multiple objects
   */
  public StorageObject copy(final String sourceBucket,
                            final String sourceObjectName,
                            final String destinationBucket,
                            final String destinationObjectName) throws IOException {

    verifyObjectSpecification(sourceBucket, sourceObjectName);
    verifyObjectSpecification(destinationBucket, destinationObjectName);

    try {
      StorageObject sourceStorageObject = Iterables.getOnlyElement(listObjects(sourceBucket, sourceObjectName, false));

      Storage.Objects.Copy copy = storage.get()
        .objects()
        .copy(
          sourceBucket,
          sourceObjectName,
          destinationBucket,
          destinationObjectName,
          null
        );

      return executeWithRetry(copy);

    } catch(IllegalArgumentException | NoSuchElementException e) {
      throw new RuntimeException("source path ({}) must refer to one and only one object");
    }
  }

  /**
   * Renames the object specified by {@code sourceBucket} and {@code sourceObjectName} to {@code destinationBucket} and
   * {@code destinationObjectName}.
   *
   * Note that this is <em>not</em> an atomic operation. Due to the way GCS works,
   * this is implemented as a copy operation followed by a deletion of the source
   * object. It is entirely possible for the copy portion to succeed while the delete
   * fails, which will leave an orphaned source object in GCS. It is the caller's
   * responsibility to recover from this situation.
   *
   * @param sourceBucket the bucket that contains the source object
   * @param sourceObjectName the name of the source object
   * @param destinationBucket the bucket to use for the destination object
   * @param destinationObjectName the name of the destination object
   * @throws IOException if anything fails
   */
  public void move(final String sourceBucket, final String sourceObjectName, final String destinationBucket, final String destinationObjectName) throws IOException {
    copy(sourceBucket, sourceObjectName, destinationBucket, destinationObjectName);
    delete(sourceBucket, sourceObjectName);
  }

  /**
   * Uploads the data contained in the supplied {@code byteSource} to the specified bucket/object.
   * @param bucket the bucket to use for the newly-uploaded object
   * @param objectName the name to use for newly-uploaded object
   * @param byteSource the source of the bytes that comprise the object
   */
  public void upload(final String bucket, final String objectName, final ByteSource byteSource) throws IOException {
    upload(bucket, objectName, byteSource, false);
  }

  /**
   * Uploads the data contained in the supplied {@code byteSource} to the specified bucket/object.
   * @param bucket the bucket to upload to
   * @param objectName the name to use for the newly-uploaded object
   * @param byteSource the source of the bytes to write
   * @param reportProgress true to report periodic upload progress to stdout.  NOTE!! The progress reporter calls
   *                       .size() on the provided {@code byteSource} in order to determine the overall size.  Do not
   *                       use the progress reporting unless you know that that underlying {@code byteSource} can be
   *                       manipulated in such a way. For example, this would cause problems if the {@code byteSource}
   *                       were a stream, and the call to .size() would cause an unwanted seek.
   */
  public void upload(final String bucket, final String objectName, final ByteSource byteSource, final boolean reportProgress)
    throws IOException {
    checkNotNull(byteSource, "byteSource");
    verifyObjectSpecification(bucket, objectName);

    final String cleanObjName = removeLeadingDelimiter(objectName);
    final RetryStrategy retryStrategy = retryStrategies.get();
    Throwable cause = null;
    Storage.Objects.Insert insertRequest = null;


    // For small files, you may wish to call setDirectUploadEnabled(true), to
    // reduce the number of HTTP requests made to the server.
    // theStorageObject.getMediaHttpUploader().setDirectUploadEnabled(true);

    try {
      while (retryStrategy.retriesRemaining()) {
        try {

          InputStreamContent content = new InputStreamContent(UPLOAD_CONTENT_TYPE, byteSource.openStream());

          // TODO: Rather than re-creating the request each time here (since the current request can't always be reused),
          //       figure out which errors are resumable and resume any in-progress uploads
          insertRequest = storage.get().objects()
            .insert(bucket,
                    new StorageObject()
                      .setName(cleanObjName)
                      .setContentDisposition(ATTACHMENT),
                    content);

          if (reportProgress) {
            insertRequest.getMediaHttpUploader()
              .setProgressListener(
                UploadProgressListener.create(cleanObjName, 1024L * 1024L, byteSource.size())
              );
          }

          insertRequest.execute();
          info("upload to {0}, {1} is complete", bucket, cleanObjName);
          return;
        } catch (Throwable e) {
          cause = e;
          if (!shouldRetry(e, retryStrategy, insertRequest)) {
            throw e;
          }
        }
      }
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    throw new RetriesExhaustedException(
      String.format("retries exhausted for request %s using strategy: %s", insertRequest, retryStrategy), cause
    );
  }

  /**
   * Downloads the data from an object and writes the bytes to the specified {@link com.google.common.io.ByteSink}
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to download
   * @param byteSink the destination to write the bytes to
   * @throws IOException
   */
  public void download(final String bucket, final String objectName, final ByteSink byteSink) throws IOException {
    download(bucket, objectName, byteSink, false);
  }

  /**
   * Downloads the data from an object and writes the bytes to the specified {@link com.google.common.io.ByteSink}
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to download
   * @param byteSink the destination to write the bytes to
   * @param reportProgress true to report periodic download progress to stdout.
   * @throws IOException
   */
  public void download(final String bucket, final String objectName, final ByteSink byteSink, final boolean reportProgress) throws IOException {
    verifyObjectSpecification(bucket, objectName);
    download(bucket, objectName, Optional.<Long>absent(), byteSink, reportProgress);
  }

  /**
   * Downloads the data from an object and writes the bytes to the specified {@link com.google.common.io.ByteSink}
   *
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to download
   * @param version  the version of the object to download
   * @param byteSink the destination to write the bytes to
   * @param reportProgress true to report periodic download progress to stdout.
   * @throws IOException
   */
  public void download(final String bucket, final String objectName, final long version, final ByteSink byteSink, final boolean reportProgress) throws IOException {
    verifyObjectSpecification(bucket, objectName, version);
    checkNotNull(byteSink, "byteSink");
  }

  private void download(final String bucket, final String objectName, final Optional<Long> version, final ByteSink byteSink, final boolean reportProgress) throws IOException {
    // For small files, you may wish to call setDirectDownloadEnabled(true), to
    // reduce the number of HTTP requests made to the server.
    // theStorageObject.getMediaHttpDownloader().setDirectDownloadEnabled(true);

    Storage.Objects.Get get = storage.get().objects()
      .get(bucket, objectName);

    if (version.isPresent()) {
      get.setGeneration(version.get());
    }

    if (reportProgress) {
      StorageObject objMetadata = get.execute();
      get.getMediaHttpDownloader()
        .setProgressListener(
          DownloadProgressListener.create(objectName, PROGRESS_INTERVAL_IN_BYTES, objMetadata.getSize().longValue())
        );
    }

    // DO NOT used the ByteSink's buffered stream capability here, as you can't guaranteee it will be flushed.
    get.executeMediaAndDownloadTo(byteSink.openStream());
  }

  /**
   * Reads the binary contents of the object, automatically decompressing gzipped objects
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to read
   * @return a {@link com.google.common.io.ByteSource} instance to read from
   * @throws IOException
   */
  public ByteSource readBinary(final String bucket, final String objectName) throws IOException {
    return readBinary(bucket, objectName, true);
  }

  /**
   * Returns a new {@link com.google.common.io.ByteSource} for reading bytes from the given object.
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to read
   * @param autoDecompress true to automatically decompress gzip files (based on file extension), false to read the "raw"
   *                       bytes.  The latter can be useful for copying files as-is, rather than decompressing and then
   *                       re-compressing.
   * @return a {@link com.google.common.io.ByteSource} instance to read from
   * @throws IOException
   */
  public ByteSource readBinary(final String bucket, final String objectName, final boolean autoDecompress) throws IOException {
    verifyObjectSpecification(bucket, objectName);
    return readBinary(bucket, objectName, Optional.<Long>absent(), autoDecompress);
  }

  /**
   * Returns a new {@link com.google.common.io.ByteSource} for reading bytes from the given object.
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to read
   * @param version the version of the object to read
   * @param autoDecompress true to automatically decompress gzip files (based on file extension), false to read the "raw"
   *                       bytes.  The latter can be useful for copying files as-is, rather than decompressing and then
   *                       re-compressing.
   * @return a {@link com.google.common.io.ByteSource} instance to read from
   * @throws IOException
   */
  public ByteSource readBinary(final String bucket, final String objectName, final long version, final boolean autoDecompress) throws IOException {
    verifyObjectSpecification(bucket, objectName, version);
    return readBinary(bucket, objectName, Optional.of(version), autoDecompress);
  }

  /**
   * Returns a new {@link com.google.common.io.ByteSource} for reading bytes from the given object.
   * @param object the object to read
   * @param autoDecompress true to automatically decompress gzip files (based on file extension), false to read the "raw"
   *                       bytes.  The latter can be useful for copying files as-is, rather than decompressing and then
   *                       re-compressing.
   * @return a {@link com.google.common.io.ByteSource} instance to read from
   * @throws IOException
   */
  public ByteSource readBinary(final StorageObject object, final boolean autoDecompress) throws IOException {
    checkNotNull(object, "object");
    return readBinary(object.getBucket(), object.getName(), object.getGeneration(), autoDecompress);
  }

  private ByteSource readBinary(final String bucket, final String objectName, final Optional<Long> version, final boolean autoDecompress) throws IOException {
    final Storage.Objects.Get get = storage.get().objects()
      .get(bucket, objectName);

    if (version.isPresent()) {
      get.setGeneration(version.get());
    }

    final InputStream inputStream = executeMediaAsInputStreamWithRetry(get, autoDecompress);

    return new ByteSource() {
      @Override
      public InputStream openStream() throws IOException {
        return inputStream;
      }
    };
  }

  /**
   * Returns a new {@link com.google.common.io.CharSource} for reading characters from the given object.
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to read
   * @param charset the character set to use when reading from the object
   * @throws IOException
   */
  public CharSource read(final String bucket, final String objectName, final Charset charset) throws IOException {
    checkNotNull(charset, "charset");
    return readBinary(bucket, objectName, true).asCharSource(charset);
  }

  /**
   * Returns a new {@link com.google.common.io.CharSource} for reading characters from the given object.
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to read
   * @param version the version of the object to read
   * @param charset the character set to use when reading from the object
   * @throws IOException
   */
  public CharSource read(final String bucket, final String objectName, final long version, final Charset charset) throws IOException {
    checkNotNull(charset, "charset");
    return readBinary(bucket, objectName, version, true).asCharSource(charset);
  }

  /**
   * Returns a new {@link com.google.common.io.CharSource} for reading characters from the given object.
   * @param object the {@link com.google.api.services.storage.model.StorageObject} to read
   * @param charset the character set to use when reading from the object
   * @throws IOException
   */
  public CharSource read(final StorageObject object, final Charset charset) throws IOException {
    checkNotNull(charset, "charset");
    return read(object.getBucket(), object.getName(), object.getGeneration(), charset);
  }

  /**
   * Composes multiple objects into a single destination object.  This behavior (combined with an upload) may be used to
   * simulate appending to a GCS object.
   * @param destinationBucket The name of the bucket to use for the composed object.  Must be the same as the bucket on
   *                          each of the source objects.
   * @param destinationObjectName The name to use for the composed object.
   * @param sourceBucket  the bucket that contains the source objects
   * @param sourceObjectNames the names of the objects to compose into the destination object
   * @return the componentCount of the newly-composed object.  GCS does not allow objects to have a componentCount
   * > 1024
   * @throws IOException
   */
  public int compose(final String destinationBucket, final String destinationObjectName, final String sourceBucket, final Collection<String> sourceObjectNames) throws IOException {
    verifyObjectSpecification(destinationBucket, destinationObjectName);
    checkArgument(!Iterables.isEmpty(sourceObjectNames), "sourceObjectNames cannot be empty");
    checkArgument(!Strings.isNullOrEmpty(sourceBucket), "sourceBucket cannot be empty");

    checkArgument(
      sourceBucket.equals(destinationBucket),
      "Cannot compose objects across buckets.  Ensure that the source and destination buckets are the same."
    );

    final class StringIsNotNullOrEmpty implements Predicate<String> {
      @Override
      public boolean apply(@Nullable String input) {
        return !com.google.common.base.Strings.isNullOrEmpty(input);
      }
    }

    checkArgument(
      Iterables.all(sourceObjectNames, new StringIsNotNullOrEmpty()),
      "Source object names cannot be null or empty"
    );

    ImmutableList<ComposeRequest.SourceObjects> sourceObjects = FluentIterable
      .from(sourceObjectNames)
      .transform(new COMPOSE_SOURCE_OBJECT_FN())
      .toList();

    StorageObject destinationObj = new StorageObject()
      .setBucket(destinationBucket)
      .setName(destinationObjectName)
      .setContentType(UPLOAD_CONTENT_TYPE);

    Storage.Objects.Compose compose = storage.get()
      .objects()
      .compose(
        destinationBucket,
        destinationObjectName,
        new ComposeRequest().setSourceObjects(sourceObjects).setDestination(destinationObj)
      );

    StorageObject response = executeWithRetry(compose);
    return response.getComponentCount();
  }

  /**
   * Deletes the specified object, if it exists.
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to delete
   * @return True if an object was deleted, false otherwise.
   * @throws IOException
   */
  public boolean delete(final String bucket, final String objectName) throws IOException {
    verifyObjectSpecification(bucket, objectName);
    return delete(bucket, objectName, Optional.<Long>absent());
  }

  /**
   * Deletes the specified object, if it exists.
   * @param bucket the bucket containing the object
   * @param objectName the name of the object to delete
   * @param version the version of the object to delete
   * @return True if an object was deleted, false otherwise.
   * @throws IOException
   */
  public boolean delete(final String bucket, final String objectName, final long version) throws IOException {
    verifyObjectSpecification(bucket, objectName, version);
    return delete(bucket, objectName, Optional.of(version));
  }

  private boolean delete(final String bucket, final String objectName, final Optional<Long> version) throws IOException {
    final Storage.Objects.Delete delete = storage
      .get()
      .objects()
      .delete(bucket, objectName);

    if (version.isPresent()) {
      delete.setGeneration(version.get());
    }

    try {
      executeWithRetry(delete);
      return true;
    } catch (Throwable e) {
      if (IS_NOT_FOUND.apply(e)) {
        // ignore; we're attempting to delete an object that doesn't exist
        return false;
      } else {
        throw e;
      }
    }
  }

  private static abstract class ProgressListener {

    protected final DecimalFormat formatter = new DecimalFormat("#,###", DecimalFormatSymbols.getInstance(Locale.getDefault()));
    protected final Stopwatch stopwatch = Stopwatch.createUnstarted();

    protected final long intervalInBytes;
    protected final String objectName;
    protected final String operation;
    protected final Optional<Long> totalSizeInBytes;
    protected long bytesTransferred = 0L;


    ProgressListener(String objectName, String operation, long intervalInBytes, Optional<Long> totalSizeInBytes) {
      checkArgument(!Strings.isNullOrEmpty(objectName), "objectName cannot be empty");
      checkArgument(intervalInBytes > 0, "intervalInBytes must be > 0");
      checkArgument(!totalSizeInBytes.isPresent() || totalSizeInBytes.get() > 0, "totalSizeInBytes must be > 0");

      this.objectName = objectName;
      this.operation = Strings.nullToEmpty(operation);
      this.intervalInBytes = intervalInBytes;
      this.totalSizeInBytes = totalSizeInBytes;
    }

    void started() {
      stopwatch.start();
      info("{0} of {1} started...", operation, objectName);
    }

    void progress(long numBytesTransferred) {
      long newBytesTransferred = numBytesTransferred;
      if ((newBytesTransferred - bytesTransferred) >= intervalInBytes) {
        if (totalSizeInBytes.isPresent()) {
          info(
            "{0} : ({1}%) {2}ed {3}/{4} bytes...",
            objectName,
            100 * newBytesTransferred / totalSizeInBytes.get(),
            operation,
            formatter.format(newBytesTransferred),
            formatter.format(totalSizeInBytes.get())
          );
        } else {
          info("{0} : {1}ed {2} bytes...", objectName, operation, formatter.format(newBytesTransferred));
        }
      }
      bytesTransferred = newBytesTransferred;
    }

    void complete() {
      if (stopwatch.isRunning()) {
        stopwatch.stop();
        info("{0} of {1} completed in {2}", operation, objectName, stopwatch);
      } else {
        info("{0} of {1} completed", operation, objectName);
      }
    }
  }

  private static class DownloadProgressListener extends ProgressListener implements MediaHttpDownloaderProgressListener {

    static DownloadProgressListener create(String objectName, long intervalInBytes) {
      return new DownloadProgressListener(objectName, intervalInBytes, Optional.<Long>absent());
    }

    static DownloadProgressListener create(String objectName, long intervalInBytes, long totalSizeInBytes) {
      checkArgument(!Strings.isNullOrEmpty(objectName), "objectName cannot be empty");
      checkArgument(intervalInBytes > 0, "intervalInBytes must be > 0");
      checkArgument(totalSizeInBytes > 0, "totalSizeInBytes must be > 0");

      return new DownloadProgressListener(objectName, intervalInBytes, Optional.of(totalSizeInBytes));
    }

    private DownloadProgressListener(String objectName, long intervalInBytes, Optional<Long> totalSizeInBytes) {
      super(objectName, "download", intervalInBytes, totalSizeInBytes);
    }

    @Override
    public void progressChanged(MediaHttpDownloader downloader) {
      switch (downloader.getDownloadState()) {
        case MEDIA_IN_PROGRESS:
          double progress = downloader.getProgress();
          progress(downloader.getNumBytesDownloaded());
          break;
        case MEDIA_COMPLETE:
          complete();
          break;
        default:
          break;
      }
    }
  }

  private static class UploadProgressListener extends ProgressListener implements MediaHttpUploaderProgressListener {

    static UploadProgressListener create(String objectName, long intervalInBytes) {
      return new UploadProgressListener(objectName, intervalInBytes, Optional.<Long>absent());
    }

    static UploadProgressListener create(String objectName, long intervalInBytes, long totalSizeInBytes) {
      checkArgument(!Strings.isNullOrEmpty(objectName), "objectName cannot be empty");
      checkArgument(intervalInBytes > 0, "intervalInBytes must be > 0");
      checkArgument(totalSizeInBytes > 0, "totalSizeInBytes must be > 0");

      return new UploadProgressListener(objectName, intervalInBytes, Optional.of(totalSizeInBytes));
    }

    private UploadProgressListener(String objectName, long intervalInBytes, Optional<Long> totalSizeInBytes) {
      super(objectName, "upload", intervalInBytes, totalSizeInBytes);
    }

    @Override
    public void progressChanged(MediaHttpUploader uploader) {
      switch (uploader.getUploadState()) {
        case INITIATION_STARTED:
          started();
          break;
        case MEDIA_IN_PROGRESS:
          progress(uploader.getNumBytesUploaded());
          break;
        case MEDIA_COMPLETE:
          complete();
          break;
        default:
          break;
      }
    }
  }

}
